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
    throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
  }

  if (ts == TransactionState::SHRINKING) {
    if (til == IsolationLevel::REPEATABLE_READ) {
      throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
    }

    if (til == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED) {
        throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
      }
    }

    if (til == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        throw TransactionAbortException(tid, AbortReason::LOCK_ON_SHRINKING);
      }
      throw TransactionAbortException(tid, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
    }
  }

  if (ts == TransactionState::GROWING) {
    if (til == IsolationLevel::READ_COMMITTED) {
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
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
  table_lock_map_latch_.unlock();

  // lock_request_queue->latch_.lock();
  std::lock_guard<std::mutex> locker(lock_request_queue->latch_);

  for (auto lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == tid) {
      if (lock_request->lock_mode_ == lock_mode) {
        return true;
      }

      // if other transaction is updating locks
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      // check upgrade conflict
      if (lock_request->lock_mode_ == LockMode::INTENTION_SHARED && (
        lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE || 
        lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE
      )) {
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      if ((lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) && (
        lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE
      )) {
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      if (lock_request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode == LockMode::EXCLUSIVE) {
        throw TransactionAbortException(tid, AbortReason::UPGRADE_CONFLICT);
      }

      // Rightnow, we can upgrade the lock
      // Relase all the locks before
      txn_manager_->ReleaseLocks(txn);
      lock_request->lock_mode_ = lock_mode;

    }
  }

  return true; 
    
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { return true; }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
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

}  // namespace bustub
