//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  child_executor_->Init();
  update_finished_ = false;
}

void UpdateExecutor::Init() {}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *table_info = catalog->GetTable(plan_->TableOid());

  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    // first delete
    auto pair = table_info->table_->GetTuple(*rid);

    pair.first.is_deleted_ = true;

    table_info->table_->UpdateTupleMeta(pair.first, *rid);

    for (const auto &index : catalog->GetTableIndexes(table_info->name_)) {
      index->index_->DeleteEntry(
          tuple->KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()), *rid,
          exec_ctx_->GetTransaction());
    }

    // Expression update ?

    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
    }

    Tuple new_tuple = Tuple{values, &child_executor_->GetOutputSchema()};

    // Insert new one
    pair.first.is_deleted_ = false;
    auto rd = table_info->table_->InsertTuple(pair.first, new_tuple, exec_ctx_->GetLockManager(),
                                              exec_ctx_->GetTransaction(), plan_->TableOid());
    if (rd.has_value()) {
      // Update index
      for (const auto &index : catalog->GetTableIndexes(table_info->name_)) {
        index->index_->InsertEntry(
            new_tuple.KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
            rd.value(), exec_ctx_->GetTransaction());
      }
    }

    count++;
  }

  Schema return_schema = Schema(std::vector<Column>{{"count", TypeId::BIGINT}});
  *tuple = Tuple(std::vector<Value>{{TypeId::BIGINT, count}}, &return_schema);

  bool return_flag = (count >= 0 && !update_finished_);
  update_finished_ = true;
  return return_flag;
}

}  // namespace bustub
