//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> locker(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    // This is an olde page. If the old page is dirty, write it back to the disk
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    }

    page_table_.erase(pages_[frame_id].GetPageId());
    // Reset the page
    pages_[frame_id].ResetMemory();
    pages_[frame_id].is_dirty_ = false;
  }

  // Get a new page id
  *page_id = AllocatePage();

  // Set the record
  page_table_[*page_id] = frame_id;
  pages_[frame_id].page_id_ = *page_id;

  // Record the access and pin the page
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // Set the pin count of the new page as 1
  pages_[frame_id].pin_count_++;

  return pages_ + frame_id;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> locker(latch_);

  auto it = page_table_.find(page_id);
  frame_id_t frame_id;

  // If the page doesn't exist, find a new frame
  if (it == page_table_.end()) {
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }

      if (pages_[frame_id].is_dirty_) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].data_);
      }

      page_table_.erase(pages_[frame_id].GetPageId());
    }

    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = page_id;
    page_table_[page_id] = frame_id;

    disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
    pages_[frame_id].is_dirty_ = false;

  } else {
    frame_id = page_table_[page_id];
  }

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;

  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> locker(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || pages_[it->second].GetPinCount() == 0) {
    return false;
  }

  pages_[it->second].pin_count_--;
  if (pages_[it->second].GetPinCount() == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  pages_[it->second].is_dirty_ |= is_dirty;

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> locker(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[it->second].GetData());
  pages_[it->second].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> locker(latch_);
  for (auto &pair : page_table_) {
    disk_manager_->WritePage(pair.first, pages_[pair.second].GetData());

    pages_[pair.second].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> locker(latch_);

  auto it = page_table_.find(page_id);

  // page not in the buffer pool
  if (it == page_table_.end()) {
    return true;
  }

  // page get pinned
  if (pages_[it->second].GetPinCount() > 0) {
    return false;
  }

  // If the page is dirty, write it back the disk
  if (pages_[it->second].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[it->second].GetData());
  }

  // reset memory
  pages_[it->second].ResetMemory();
  pages_[it->second].is_dirty_ = false;
  pages_[it->second].pin_count_ = 0;

  // stop tracking in the lru_replacer
  replacer_->Remove(it->second);

  // add the frame_id into the free list
  free_list_.push_back(it->second);

  // delete from page table
  page_table_.erase(page_id);

  // Delocate page
  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::GetReplacerCurrSize() -> size_t { return 0; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);

  return {this, page};
}

}  // namespace bustub
