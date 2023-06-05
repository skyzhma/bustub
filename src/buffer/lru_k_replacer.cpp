//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include "common/exception.h"

namespace bustub {

// size_t inf_flag =  std::numeric_limits<size_t>::max();

#define inf_flag std::numeric_limits<size_t>::max()

LRUKNode::LRUKNode(frame_id_t fid, size_t cur_time_stamp, size_t k) {
  fid_ = fid;
  k_ = k;
  is_evictable_ = true;
  history_.push_back(cur_time_stamp);
}

void LRUKNode::SetEvictable(bool status) { is_evictable_ = status; }

auto LRUKNode::IsEvictable() -> bool { return is_evictable_; }

void LRUKNode::AddHistory(size_t cur_time_stamp) {
  if (history_.size() == k_) {
    history_.pop_front();
  }
  history_.push_back(cur_time_stamp);
}

auto LRUKNode::GetTimeStamp() -> size_t {
  if (history_.size() == k_) {
    return GetFrontHistory();
  }
  return inf_flag;
}

auto LRUKNode::GetFrontHistory() -> size_t { return history_.front(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

LRUKReplacer::~LRUKReplacer() {
  for (auto &pair : node_store_) {
    delete pair.second;
  }
  node_store_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  size_t time_stamp = inf_flag;
  bool inf = false;
  LRUKNode *node;
  bool status = false;

  std::lock_guard<std::mutex> locker(latch_);

  for (auto &pair : node_store_) {
    if (!pair.second->IsEvictable()) {
      continue;
    }

    size_t tmp = pair.second->GetTimeStamp();

    if (!inf) {
      if (tmp == inf_flag) {
        time_stamp = pair.second->GetFrontHistory();
        *frame_id = pair.first;
        node = pair.second;
        inf = true;
      } else if (tmp < time_stamp) {
        time_stamp = tmp;
        node = pair.second;
        *frame_id = pair.first;
      }

    } else if (tmp == inf_flag) {
      if (pair.second->GetFrontHistory() < time_stamp) {
        time_stamp = pair.second->GetFrontHistory();
        node = pair.second;
        *frame_id = pair.first;
      }
    }
  }

  if (time_stamp != inf_flag) {
    delete node;
    node_store_.erase(*frame_id);
    status = true;
    curr_size_--;
  }

  return status;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> locker(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    if (node_store_.size() == replacer_size_) {
      frame_id_t *tmp = nullptr;
      if (!Evict(tmp)) {
        return;
      }
    }
    auto *node = new LRUKNode(frame_id, current_timestamp_++, k_);
    curr_size_++;
    node_store_.emplace(frame_id, node);
  } else {
    node_store_.at(frame_id)->AddHistory(current_timestamp_++);
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> locker(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    throw Exception("The frame_id is not in the list");
  }

  bool is_evictable = node_store_.at(frame_id)->IsEvictable();
  if (!is_evictable && set_evictable) {
    curr_size_++;
  } else if (is_evictable && !set_evictable) {
    curr_size_--;
  }
  node_store_.at(frame_id)->SetEvictable(set_evictable);
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> locker(latch_);

  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }

  auto *node = it->second;
  if (node->IsEvictable()) {
    curr_size_--;
    delete node;
    node_store_.erase(frame_id);
  } else {
    throw Exception("Can't remove frame that's inevictable");
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> locker(latch_);
  size_t tmp = curr_size_;
  return tmp;
}

}  // namespace bustub
