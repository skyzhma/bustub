//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  Init();

}

void NestedLoopJoinExecutor::Init() { 

  left_executor_->Init();
  right_executor_->Init();
  left_finished_ = false;
  right_finished_ = false;
  right_index_ = 0;

  Tuple tuple{};
  RID rid{};
  while (right_executor_->Next(&tuple, &rid)) {
    right_tuples_.emplace_back(tuple);
  }

 }

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  Tuple left_tuple{};
  RID left_rid{};
  bool flag = false;
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    
    // join with right table
    auto right_tuple = right_tuples_[right_index_];
    right_index_ = (right_index_ + 1) % right_tuples_.capacity();

    std::vector<Value> values;
    // match succeed
    for (size_t t = 0; t < left_executor_->GetOutputSchema().GetColumns().size(); t++) {
      values.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), t));
    }

    if (plan_->Predicate()->EvaluateJoin(&left_tuple, 
                                        left_executor_->GetOutputSchema(), 
                                        &right_tuple, 
                                        right_executor_->GetOutputSchema()).GetAs<bool>()) {
      
      // match succeed
      for (size_t t = 0; t < right_executor_->GetOutputSchema().GetColumns().size(); t++) {
        values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), t));
      }
    } else if(plan_->GetJoinType() == JoinType::LEFT) {
      // Left Join
      for (size_t t = 0; t < right_executor_->GetOutputSchema().GetColumns().size(); t++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(t).GetType()));
      }

    } 

    // schema
    *tuple = Tuple(values, &GetOutputSchema());
    flag = true;
    break;
  }

  return flag;
}

}  // namespace bustub
