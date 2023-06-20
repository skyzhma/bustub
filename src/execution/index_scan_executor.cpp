//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  index_oid_t index_id = plan_->GetIndexOid();

  IndexInfo *index_info = exec_ctx_->GetCatalog()->GetIndex(index_id);

  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);

  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());

  iter_ = tree_->GetBeginIterator();
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_.IsEnd()) {
    return false;
  }

  auto pair = table_info_->table_->GetTuple((*iter_).second);

  while (pair.first.is_deleted_) {
    ++iter_;
    if (iter_.IsEnd()) {
      return false;
    }
    pair = table_info_->table_->GetTuple((*iter_).second);
  }

  *tuple = pair.second;
  *rid = (*iter_).second;

  ++iter_;
  return true;
}

}  // namespace bustub
