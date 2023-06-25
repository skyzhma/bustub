//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  delete_finished_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->TableOid());

  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    auto pair = table_info->table_->GetTuple(*rid);

    pair.first.is_deleted_ = true;

    table_info->table_->UpdateTupleMeta(pair.first, *rid);

    // Update index
    for (const auto &index : catalog->GetTableIndexes(table_info->name_)) {
      index->index_->DeleteEntry(
          tuple->KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid,
          exec_ctx_->GetTransaction());
    }

    count++;
  }

  Schema return_schema = Schema(std::vector<Column>{{"count", TypeId::BIGINT}});
  *tuple = Tuple(std::vector<Value>{{TypeId::BIGINT, count}}, &return_schema);

  bool return_flag = (count >= 0 && !delete_finished_);
  delete_finished_ = true;
  return return_flag;
}

}  // namespace bustub
