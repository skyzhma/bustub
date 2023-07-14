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
  std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
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

      // Rightnow, we can upgrade the lock
      // Relase current held locks

      // switch (lock_request->lock_mode_) {
      //   case LockMode::SHARED: {
      //     txn->GetSharedTableLockSet()->erase(oid);
      //     break;
      //   }
      //   case LockMode::INTENTION_SHARED: {
      //     txn->GetIntentionSharedTableLockSet()->erase(oid);
      //     break;
      //   }
      //   case LockMode::EXCLUSIVE: {
      //     txn->GetExclusiveTableLockSet()->erase(oid);
      //     break;
      //   }
      //   case LockMode::INTENTION_EXCLUSIVE: {
      //     txn->GetIntentionExclusiveTableLockSet()->erase(oid);
      //     break;
      //   }
      //   case LockMode::SHARED_INTENTION_EXCLUSIVE: {
      //     txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
      //     break;
      //   }
      // }

      // Release current lock
      UnlockTable(txn, oid);

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

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  while (!GrantTableLock(lock_request_queue, lock_request, oid)) {
    lock_request_queue->cv_.wait(lock);
  }

  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // check if the transaction hold row locks
  auto shared_row_lock_set = txn->GetSharedRowLockSet();
  if (shared_row_lock_set->find(oid) != shared_row_lock_set->end()) {
    return false;
  }

  auto exclusive_row_lock_set = txn->GetExclusiveRowLockSet();
  if (exclusive_row_lock_set->find(oid) != exclusive_row_lock_set->end()) {
    return false;
  }

  // lock the table map
  // std::lock_guard<std::mutex> locker(table_lock_map_latch_);
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
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

    // remove request 
    lock_request_queue->request_queue_.remove(request);

    // Book Keeping,  delete corresponding lock
    ReleaseTableLock(txn, oid, request->lock_mode_);
    break;
  }

  if (!flag) {
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
  if (lock_mode == LockMode::EXCLUSIVE) {
    int num = 0;
    auto table_lock_set1 = txn->GetExclusiveTableLockSet();
    if (table_lock_set1->find(oid) == table_lock_set1->end()) { num++; }
    
    auto table_lock_set2 = txn->GetIntentionExclusiveTableLockSet();
    if (table_lock_set2->find(oid) == table_lock_set2->end()) { num++; }
  
    auto table_lock_set3 = txn->GetSharedIntentionExclusiveTableLockSet();
    if (table_lock_set3->find(oid) == table_lock_set3->end()) { num++; }

    if (num == 3) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(tid, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  if (lock_mode == LockMode::SHARED) {
    int num = 0;
    auto table_lock_set1 = txn->GetSharedTableLockSet();
    if (table_lock_set1->find(oid) == table_lock_set1->end()) { num++; }
    
    auto table_lock_set2 = txn->GetIntentionSharedTableLockSet();
    if (table_lock_set2->find(oid) == table_lock_set2->end()) { num++; }
  
    auto table_lock_set3 = txn->GetSharedIntentionExclusiveTableLockSet();
    if (table_lock_set3->find(oid) == table_lock_set3->end()) { num++; }

    if (num == 3) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(tid, AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }

  // Check Isolation level
  if (txn->GetState() == TransactionState::SHRINKING) {
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
  std::lock_guard<std::mutex> locker(lock_request_queue->latch_);
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

  std::unique_lock<std::mutex> lock(lock_request_queue->latch_);
  // Change it to GrantRowLock
  while (!GrantTableLock(lock_request_queue, lock_request, oid)) {
    lock_request_queue->cv_.wait(lock);
  }
  
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
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

  if (l1 == LockMode::SHARED && l2 == LockMode::SHARED) {
    return true;
  }

  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE && l2 == LockMode::INTENTION_SHARED) {
    return true;
  }

  return false;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  if (curr_lock_mode == LockMode::INTENTION_SHARED &&
      (requested_lock_mode == LockMode::SHARED || requested_lock_mode == LockMode::EXCLUSIVE ||
       requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
       requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
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

auto LockManager::GrantTableLock(std::shared_ptr<LockRequestQueue> &queue, std::shared_ptr<LockRequest> &request,
                                 const table_oid_t &oid) -> bool {
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
      AcquireTableLock(txn_manager_->GetTransaction(queue_request->txn_id_), oid, queue_request->lock_mode_);
    }
  }

  if (update_current_txn) {
    queue->upgrading_ = INVALID_TXN_ID;
  }

  request->granted_ = true;
  AcquireTableLock(txn_manager_->GetTransaction(request->txn_id_), oid, request->lock_mode_);

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
    auto row_lock_set = txn->GetSharedRowLockSet()->at(oid);
    row_lock_set.insert(rid);
  } else {
    auto row_lock_set = txn->GetExclusiveRowLockSet()->at(oid);
    row_lock_set.insert(rid);
  }
}

}  // namespace bustub
