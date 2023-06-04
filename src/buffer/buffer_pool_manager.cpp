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

  latch_.lock();

  // find a free frame id
  frame_id_t free_frame_id;
  if (!free_list_.empty()) {
    free_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&free_frame_id)) {
      latch_.unlock();
      return nullptr;
    }
  }

  page_id_t free_frame_page_id;

  // If the free frame corresponds to a page, find the page id and erase the record in page_table_
  // Besides, flush the page if the page is dirty
  for (auto& pair : page_table_) {
    if (pair.second == free_frame_id) {
      free_frame_page_id = pair.first;
      if (pages_[free_frame_id].IsDirty()) {
        disk_manager_->WritePage(free_frame_page_id, pages_[free_frame_id].GetData());
      }
      page_table_.erase(free_frame_page_id);
      break;
    }
  }

  // Reset the page
  pages_[free_frame_id].ResetMemory();

  // Get a new page id
  *page_id = AllocatePage();

  // Set the record
  page_table_[*page_id] = free_frame_id;

  // Record the access and pin the page
  replacer_->RecordAccess(free_frame_id);
  replacer_->SetEvictable(free_frame_id, false);

  // Set the pin count of the new page as 1
  pages_[free_frame_id].pin_count_++;

  latch_.unlock();  
  
  return pages_ + free_frame_id; 
  
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {

  latch_.lock();

  auto it = page_table_.find(page_id);
  frame_id_t frame_id;
  page_id_t old_page_id;

  // disk_manager_->WritePage(4, pages_[4].data_);
  // disk_manager_->ReadPage(page_id, pages_[4].data_);


  // If the page doesn't exist, find a new frame
  if (it == page_table_.end()) {
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
      
    } else {
      if (!replacer_->Evict(&frame_id)) {
        latch_.unlock();
        return nullptr;
      }

      for (auto& pair : page_table_) {
        if (pair.second == frame_id) {
          old_page_id = pair.first;
          break;
        }
      }

      if (pages_[frame_id].is_dirty_) {
        disk_manager_->WritePage(old_page_id, pages_[frame_id].data_);
      }

      pages_[frame_id].ResetMemory();
      page_table_.erase(old_page_id);
      page_table_[page_id] = frame_id;
    }
  } else {
    frame_id = page_table_[page_id];
  }

  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_++;

  latch_.unlock();

  return pages_ + frame_id;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  
  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it == page_table_.end() || pages_[page_table_[page_id]].GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  } 

  pages_[page_table_[page_id]].pin_count_--;
  if (pages_[page_table_[page_id]].GetPinCount() == 0) {
    replacer_->SetEvictable(page_table_[page_id], true);
  }
  pages_[page_table_[page_id]].is_dirty_ = is_dirty;

  latch_.unlock();

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 

  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  } 

  disk_manager_->WritePage(page_id, pages_[it->second].GetData());

  pages_[it->second].is_dirty_ = false;

  latch_.unlock();

  return true; 
}

void BufferPoolManager::FlushAllPages() {

  for (auto& pair : page_table_) {
    FlushPage(pair.first);
  }

}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 

  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  if (pages_[it->second].GetPinCount() != 0) {
    latch_.unlock();
    return false;
  }

  // delete from page table
  page_table_.erase(page_id);

  // stop track in the lru_replacer
  replacer_->Remove(it->second);

  // add the frame_id into the free list
  free_list_.push_back(it->second);

  // reset memory
  pages_[it->second].ResetMemory();

  // Delocate page
  DeallocatePage(page_id);

  latch_.unlock();

  return true; 
  
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
