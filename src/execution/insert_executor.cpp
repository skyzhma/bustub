//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  child_executor_->Init();
  insert_finished_ = true;
}

void InsertExecutor::Init() {}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Catalog *catalog = exec_ctx_->GetCatalog();

  // insert the tuple to the table
  TableInfo *table_info = catalog->GetTable(plan_->TableOid());

  TupleMeta meta;
  meta.delete_txn_id_ = INVALID_TXN_ID;
  meta.insert_txn_id_ = INVALID_TXN_ID;
  meta.is_deleted_ = false;
  int count = 0;

  while (child_executor_->Next(tuple, rid)) {
    // Insert into the tuple
    auto rd = table_info->table_->InsertTuple(meta, *tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                              plan_->TableOid());

    // Update index
    for (const auto &index : catalog->GetTableIndexes(table_info->name_)) {
      index->index_->InsertEntry(
          tuple->KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
          rd.value(), exec_ctx_->GetTransaction());
    }

    count++;
  }

  Schema return_schema = Schema(std::vector<Column>{{"count", TypeId::BIGINT}});
  *tuple = Tuple(std::vector<Value>{{TypeId::BIGINT, count}}, &return_schema);

  bool return_flag = (count >= 0 && insert_finished_);
  insert_finished_ = false;
  return return_flag;
}

}  // namespace bustub
