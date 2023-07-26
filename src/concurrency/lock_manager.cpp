//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  txn_id_t tid = txn->GetTransactionId();
  TransactionState ts = txn->GetState();
  IsolationLevel til = txn->GetIsolationLevel();

  if (ts == TransactionState::COMMITTED || ts == TransactionState::ABORTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
  }

  if (ts == TransactionState::SHRINKING) {
    if (til == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
    }

    if (til == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
      }
    }

    if (til == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(tid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  if (ts == TransactionState::GROWING) {
    if (til == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }

  table_lock_map_latch_.lock();
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  if (table_lock_map_.find(oid) != table_lock_map_.end()) {
    lock_request_queue = table_lock_map_[oid];
  } else {
    lock_request_queue = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = lock_request_queue;
  }

  // lock_request_queue->latch_.lock();
  // std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  bool flag = false;
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == tid) {
      flag = true;

      if (lock_request->lock_mode_ == lock_mode) {
        return true;
      }

      // if other transaction is updating locks
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      // check upgrade conflict
      if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      // Release current lock
      ReleaseTableLock(txn, oid, lock_request->lock_mode_);

      // remove the request
      lock_request_queue->request_queue_.remove(lock_request);
      break;
    }
  }

  // add a new lock request
  auto lock_request = std::make_shared<LockRequest>(tid, lock_mode, oid);

  if (flag) {
    // upgrade lock
    lock_request->lock_mode_ = lock_mode;
    lock_request_queue->upgrading_ = tid;
    lock_request->granted_ = false;
  }

  lock_request_queue->request_queue_.emplace_back(lock_request);

  // lock_request_queue->latch_.unlock();
  // std::unique_lock<std::mutex> lock(lock_request_queue->latch_);

  while (!GrantTableLock(txn, lock_request_queue, lock_request, oid)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // txn_manager_->Abort(txn);
      lock_request_queue->request_queue_.remove(lock_request);
      return false;
    }
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // check if the transaction hold row locks
  auto shared_row_lock_set = txn->GetSharedRowLockSet();
  if (shared_row_lock_set->find(oid) != shared_row_lock_set->end() && !shared_row_lock_set->at(oid).empty()) {
    return false;
  }

  auto exclusive_row_lock_set = txn->GetExclusiveRowLockSet();
  if (exclusive_row_lock_set->find(oid) != exclusive_row_lock_set->end() && !exclusive_row_lock_set->at(oid).empty()) {
    return false;
  }

  // lock the table map
  // std::lock_guard<std::mutex> locker(table_lock_map_latch_);
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue;
  lock_request_queue = table_lock_map_[oid];

  std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
  table_lock_map_latch_.unlock();

  // find the txn
  auto flag = false;

  for (auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }

    flag = true;

    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
        (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) {
      txn->SetState(TransactionState::SHRINKING);
    }

    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
        txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (request->lock_mode_ == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::SHRINKING);
      }
    }

    // Book Keeping,  delete corresponding lock
    ReleaseTableLock(txn, oid, request->lock_mode_);

    // remove request
    lock_request_queue->request_queue_.remove(request);

    break;
  }

  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // wake up the other threads blocked on this table
  lock_request_queue->cv_.notify_all();

  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  txn_id_t tid = txn->GetTransactionId();
  TransactionState ts = txn->GetState();
  IsolationLevel til = txn->GetIsolationLevel();

  // Not support Intension locks
  if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(tid, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }

  // ensure it hold a table lock

  int num = 0;

  auto table_lock_set1 = txn->GetExclusiveTableLockSet();
  if (table_lock_set1->find(oid) == table_lock_set1->end()) {
    num++;
  }

  auto table_lock_set2 = txn->GetIntentionExclusiveTableLockSet();
  if (table_lock_set2->find(oid) == table_lock_set2->end()) {
    num++;
  }

  auto table_lock_set3 = txn->GetSharedTableLockSet();
  if (table_lock_set3->find(oid) == table_lock_set3->end()) {
    num++;
  }

  auto table_lock_set4 = txn->GetIntentionSharedTableLockSet();
  if (table_lock_set4->find(oid) == table_lock_set4->end()) {
    num++;
  }

  auto table_lock_set5 = txn->GetSharedIntentionExclusiveTableLockSet();
  if (table_lock_set5->find(oid) == table_lock_set5->end()) {
    num++;
  }

  if (num == 5) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(tid, AbortReason::TABLE_LOCK_NOT_PRESENT);
  }

  // Check Isolation level
  if (ts == TransactionState::SHRINKING) {
    if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
    }
  }

  // copy from table lock, check isolation level
  if (ts == TransactionState::COMMITTED || ts == TransactionState::ABORTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
  }

  if (ts == TransactionState::SHRINKING) {
    if (til == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
    }

    if (til == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::INTENTION_SHARED && lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
      }
    }

    if (til == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
      }
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(tid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  if (ts == TransactionState::GROWING) {
    if (til == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
  }

  // lock row lock map
  row_lock_map_latch_.lock();

  // take row lock request queue
  std::shared_ptr<LockRequestQueue> lock_request_queue;
  if (row_lock_map_.find(rid) != row_lock_map_.end()) {
    lock_request_queue = row_lock_map_[rid];
  } else {
    lock_request_queue = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = lock_request_queue;
  }

  // lock_request_queue->latch_.lock();
  // std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  bool flag = false;
  for (const auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == tid) {
      flag = true;

      if (lock_request->lock_mode_ == lock_mode) {
        return true;
      }

      // if other transaction is updating locks
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      // check upgrade conflict
      if (!CanLockUpgrade(lock_request->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      // problem of unlock
      UnlockRow(txn, oid, rid, true);

      // remove the request
      lock_request_queue->request_queue_.remove(lock_request);
      break;
    }
  }

  // add a new lock request
  auto lock_request = std::make_shared<LockRequest>(tid, lock_mode, oid, rid);

  if (flag) {
    // upgrade lock
    lock_request->lock_mode_ = lock_mode;
    lock_request_queue->upgrading_ = tid;
    lock_request->granted_ = false;
  }

  lock_request_queue->request_queue_.emplace_back(lock_request);

  // std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  // Change it to GrantRowLock
  while (!GrantRowLock(txn, lock_request_queue, lock_request, oid, rid)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      // txn_manager_->Abort(txn);
      lock_request_queue->request_queue_.remove(lock_request);
      return false;
    }
  }

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  // lock the table map
  // std::lock_guard<std::mutex> locker(table_lock_map_latch_);

  if (!txn->IsTableExclusiveLocked(oid) &&
      !txn->IsTableSharedLocked(oid) &&
      !txn->IsTableIntentionExclusiveLocked(oid) &&
      !txn->IsTableIntentionSharedLocked(oid) &&
      !txn->IsTableIntentionSharedLocked(oid)) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  std::shared_ptr<LockRequestQueue> lock_request_queue;
  lock_request_queue = row_lock_map_[rid];

  std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
  row_lock_map_latch_.unlock();

  // find the txn
  auto flag = false;

  for (auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ != txn->GetTransactionId()) {
      continue;
    }

    if (request->oid_ != oid) {
      continue;
    }

    if (request->rid_ == rid) {
      flag = true;

      if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
          (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::EXCLUSIVE)) {
        txn->SetState(TransactionState::SHRINKING);
      }

      if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
          txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
        if (request->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      // Book Keeping,  delete corresponding lock
      ReleaseRowLock(txn, oid, rid, request->lock_mode_);

      // remove request
      lock_request_queue->request_queue_.remove(request);

      break;
    }
  }

  if (!flag) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }

  // wake up the other threads blocked on this table
  lock_request_queue->cv_.notify_all();

  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  if (waits_for_.find(t1) == waits_for_.end()) {
    waits_for_.insert({t1, std::vector<txn_id_t>()});
  }

  size_t i = BinarySearch(waits_for_[t1], t2);
  if (i == waits_for_[t1].size() || waits_for_[t1][i] != t2) {
    waits_for_[t1].insert(waits_for_[t1].begin() + i, t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  size_t i = BinarySearch(waits_for_[t1], t2);

  if (i < waits_for_[t1].size() && waits_for_[t1][i] == t2) {
    waits_for_[t1].erase(waits_for_[t1].begin() + i);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  bool flag = false;
  txn_id_t lowest_tid = 2147483647;

  for (auto &pair : waits_for_) {
    lowest_tid = std::min(lowest_tid, pair.first);
  }

  std::vector<txn_id_t> path;
  std::unordered_set<txn_id_t> visit;
  txn_id_t tmp;

  DFS(lowest_tid, tmp, path, visit, flag);

  if (flag) {
    bool start = false;

    for (auto tid : path) {
      if (tid == tmp) {
        start = true;
      }
      if (start) {
        tmp = std::max(tmp, tid);
      }
    }

    *txn_id = tmp;
  }

  return flag;
}

void LockManager::DFS(txn_id_t tid, txn_id_t &entrance, std::vector<txn_id_t> &path,
                      std::unordered_set<txn_id_t> &visit, bool &flag) {
  if (visit.find(tid) != visit.end()) {
    entrance = tid;
    flag = true;
    return;
  }

  visit.insert(tid);
  path.emplace_back(tid);
  for (auto next : waits_for_[tid]) {
    DFS(next, entrance, path, visit, flag);
    if (flag) {
      return;
    }
  }
  visit.erase(tid);
  path.pop_back();
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);

  for (auto &pair : waits_for_) {
    for (auto &value : pair.second) {
      edges.emplace_back(pair.first, value);
    }
  }

  return edges;
}

void LockManager::BuildGraph(std::list<std::shared_ptr<LockRequest>> queue) {
  for (auto i = queue.begin(); i != queue.end(); i++) {
    if (txn_manager_->GetTransaction((*i)->txn_id_)->GetState() == TransactionState::ABORTED) {
      continue;
    }

    for (auto j = queue.begin(); j != i; j++) {
      if (txn_manager_->GetTransaction((*j)->txn_id_)->GetState() == TransactionState::ABORTED) {
        continue;
      }

      if ((*i)->granted_ && !((*j)->granted_)) {
        AddEdge((*j)->txn_id_, (*i)->txn_id_);
      }

      if ((*j)->granted_ && !((*i)->granted_)) {
        AddEdge((*i)->txn_id_, (*j)->txn_id_);
      }
    }
  }
}

void LockManager::RemoveTableRequest(std::shared_ptr<LockRequestQueue> &lock_request_queue, txn_id_t txn_id_) {
  std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
  for (auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn_id_) {
      lock_request_queue->request_queue_.remove(request);
      break;
    }
  }
  lock_request_queue->cv_.notify_all();
}

void LockManager::RemoveRowRequest(std::shared_ptr<LockRequestQueue> &lock_request_queue, txn_id_t txn_id_,
                                   table_oid_t oid, RID rid) {
  std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
  for (auto &request : lock_request_queue->request_queue_) {
    if (request->txn_id_ == txn_id_ && request->oid_ == oid && request->rid_ == rid) {
      lock_request_queue->request_queue_.remove(request);
      break;
    }
  }
  lock_request_queue->cv_.notify_all();
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    waits_for_.clear();
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock

      // build graph
      table_lock_map_latch_.lock();
      for (auto &pair : table_lock_map_) {
        BuildGraph(pair.second->request_queue_);
      }

      row_lock_map_latch_.lock();
      for (auto &pair : row_lock_map_) {
        BuildGraph(pair.second->request_queue_);
      }

      txn_id_t tid;
      while (HasCycle(&tid)) {
        auto txn = txn_manager_->GetTransaction(tid);

        // Abort the transaction (set abort state and release locks)
        txn->SetState(TransactionState::ABORTED);

        std::cout << txn << " " << txn->GetExclusiveRowLockSet()->size() << std::endl;

        for (auto oid : *txn->GetSharedTableLockSet()) {
          RemoveTableRequest(table_lock_map_[oid], txn->GetTransactionId());
        }

        txn->GetSharedTableLockSet()->clear();

        for (auto oid : *txn->GetExclusiveTableLockSet()) {
          RemoveTableRequest(table_lock_map_[oid], txn->GetTransactionId());
        }

        txn->GetExclusiveTableLockSet()->clear();

        for (auto oid : *txn->GetIntentionSharedTableLockSet()) {
          RemoveTableRequest(table_lock_map_[oid], txn->GetTransactionId());
        }

        txn->GetIntentionSharedTableLockSet()->clear();

        for (auto oid : *txn->GetIntentionExclusiveTableLockSet()) {
          RemoveTableRequest(table_lock_map_[oid], txn->GetTransactionId());
        }

        txn->GetIntentionExclusiveTableLockSet()->clear();

        for (auto oid : *txn->GetSharedIntentionExclusiveTableLockSet()) {
          RemoveTableRequest(table_lock_map_[oid], txn->GetTransactionId());
        }

        txn->GetSharedIntentionExclusiveTableLockSet()->clear();

        for (auto &pair : *txn->GetSharedRowLockSet()) {
          for (auto &rid : pair.second) {
            RemoveRowRequest(row_lock_map_[rid], txn->GetTransactionId(), pair.first, rid);
          }
        }

        txn->GetSharedRowLockSet()->clear();

        for (auto &pair : *txn->GetExclusiveRowLockSet()) {
          for (auto &rid : pair.second) {
            RemoveRowRequest(row_lock_map_[rid], txn->GetTransactionId(), pair.first, rid);
          }
        }

        txn->GetExclusiveRowLockSet()->clear();

        // Remove edge
        auto it = waits_for_.find(tid);
        if (it != waits_for_.end()) {
          waits_for_.erase(it);
        }
        for (auto pair : waits_for_) {
          if (pair.second.empty()) {
            continue;
          }
          size_t ind = BinarySearch(pair.second, tid);
          if (pair.second[ind] == tid) {
            pair.second.erase(pair.second.begin() + ind);
          }
        }
      }

      table_lock_map_latch_.unlock();
      row_lock_map_latch_.unlock();
    }
  }
}

auto LockManager::BinarySearch(std::vector<txn_id_t> &neighbors, txn_id_t tid) -> size_t {
  size_t i = 0;
  size_t j = neighbors.size();
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (neighbors[mid] >= tid) {
      j = mid;
    } else {
      i = mid + 1;
    }
  }

  return i;
}

auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  // check conflict
  if (l1 == LockMode::INTENTION_SHARED && l2 != LockMode::EXCLUSIVE) {
    return true;
  }

  if (l1 == LockMode::INTENTION_EXCLUSIVE &&
      (l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE)) {
    return true;
  }

  if (l1 == LockMode::SHARED && (l2 == LockMode::SHARED || l2 == LockMode::INTENTION_SHARED)) {
    return true;
  }

  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE && l2 == LockMode::INTENTION_SHARED) {
    return true;
  }

  return false;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {
    return true;
  }

  if ((curr_lock_mode == LockMode::SHARED || curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) &&
      (requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
    return true;
  }

  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE && requested_lock_mode == LockMode::EXCLUSIVE) {
    return true;
  }

  return false;
}

auto LockManager::GrantTableLock(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue,
                                 std::shared_ptr<LockRequest> &request, const table_oid_t &oid) -> bool {
  bool wait_compatibility = true;
  bool current = false;
  for (auto &queue_request : queue->request_queue_) {
    if (queue_request == request) {
      current = true;
      continue;
    }

    bool flag = AreLocksCompatible(queue_request->lock_mode_, request->lock_mode_);
    if (queue_request->granted_) {
      if (flag) {
        continue;
      }
      return false;
    }

    // waiting
    if (!flag) {
      if (!current) {
        wait_compatibility = false;
        break;
      }
    }
  }

  auto update_current_txn = (queue->upgrading_ == request->txn_id_);

  if (!wait_compatibility && !update_current_txn) {
    return false;
  }

  for (auto &queue_request : queue->request_queue_) {
    if (queue_request == request) {
      continue;
    }

    if (!queue_request->granted_ && AreLocksCompatible(queue_request->lock_mode_, request->lock_mode_)) {
      queue_request->granted_ = true;
      AcquireTableLock(txn, oid, queue_request->lock_mode_);
    }
  }

  if (update_current_txn) {
    queue->upgrading_ = INVALID_TXN_ID;
  }

  request->granted_ = true;
  AcquireTableLock(txn, oid, request->lock_mode_);

  return true;
}

auto LockManager::GrantRowLock(Transaction *txn, std::shared_ptr<LockRequestQueue> &queue,
                               std::shared_ptr<LockRequest> &request, const table_oid_t &oid, const RID &rid) -> bool {
  for (auto &queue_request : queue->request_queue_) {
    if (queue_request == request) {
      continue;
    }

    if (queue_request->granted_) {
      if (queue_request->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }

      if (request->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
  }

  request->granted_ = true;
  AcquireRowLock(txn, request->lock_mode_, oid, rid);

  return true;
}

void LockManager::ReleaseTableLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}

void LockManager::ReleaseRowLock(Transaction *txn, const table_oid_t &oid, const RID &rid, LockMode lock_mode) {
  if (lock_mode == LockMode::SHARED) {
    auto shared_lock_set = txn->GetSharedRowLockSet();
    if (shared_lock_set->find(oid) != shared_lock_set->end()) {
      if (shared_lock_set->at(oid).find(rid) != shared_lock_set->at(oid).end()) {
        shared_lock_set->at(oid).erase(rid);
      }
    }

    return;
  }

  auto exclusive_lock_set = txn->GetExclusiveRowLockSet();
  if (exclusive_lock_set->find(oid) != exclusive_lock_set->end()) {
    if (exclusive_lock_set->at(oid).find(rid) != exclusive_lock_set->at(oid).end()) {
      exclusive_lock_set->at(oid).erase(rid);
    }
  }
}

void LockManager::AcquireTableLock(Transaction *txn, const table_oid_t &oid, LockMode lock_mode) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

void LockManager::AcquireRowLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    auto row_lock_set = txn->GetSharedRowLockSet().get();
    if (row_lock_set->find(oid) == row_lock_set->end()) {
      // auto shared_rid_set = std::make_shared<std::unordered_set<RID>>();
      // shared_rid_set->insert(rid);
      auto rid_set = std::unordered_set<RID>();
      rid_set.insert(rid);
      row_lock_set->insert({oid, rid_set});
      return;
    }

    txn->GetSharedRowLockSet()->at(oid).insert(rid);
  } else {
    auto row_lock_set = txn->GetExclusiveRowLockSet();
    if (row_lock_set->find(oid) == row_lock_set->end()) {
      auto rid_set = std::unordered_set<RID>();
      rid_set.insert(rid);
      row_lock_set->insert({oid, rid_set});
      return;
    }

    txn->GetExclusiveRowLockSet()->at(oid).insert(rid);
  }
}

}  // namespace bustub
