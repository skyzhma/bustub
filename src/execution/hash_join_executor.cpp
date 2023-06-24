//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_child)), right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  right_child_->Init();
  Tuple tuple{};
  RID rid{};
  
  auto right_expr = plan_->RightJoinKeyExpressions();

  while (right_child_->Next(&tuple, &rid)) {

    Value value = right_expr[0]->Evaluate(&tuple, plan_->GetRightPlan()->OutputSchema());
    ht_.Insert(JointKey{value}, tuple);
  }


}

void HashJoinExecutor::Init() { 

  left_child_->Init();
  right_child_->Init();

}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  Tuple left_tuple{};
  RID left_rid{};

  while (true) {

    if (!tuples_.empty()) {
      *tuple = tuples_.back();
      tuples_.pop_back();
      return true;
    }

    if (!left_child_->Next(&left_tuple, &left_rid)) {
      return false;
    }

    Value value = plan_->RightJoinKeyExpressions()[0]->Evaluate(&left_tuple, plan_->GetRightPlan()->OutputSchema());
    auto return_tuples = ht_.Get(JointKey{value});
    if (return_tuples.empty()) {

      if (plan_->GetJoinType() == JoinType::LEFT) {
        std::vector<Value> values;

        // match succeed
        for (size_t t = 0; t < left_child_->GetOutputSchema().GetColumns().size(); t++) {
          values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), t));
        }

        for (size_t t = 0; t < right_child_->GetOutputSchema().GetColumns().size(); t++) {
          values.emplace_back(
              ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(t).GetType()));
        }

        *tuple = Tuple(values, &GetOutputSchema());
        return true;

      }

    } else {

      for (auto &tuple : return_tuples) {
        
        std::vector<Value> values;
        // match succeed
        for (size_t t = 0; t < left_child_->GetOutputSchema().GetColumns().size(); t++) {
          values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), t));
        }

        for (size_t t = 0; t < right_child_->GetOutputSchema().GetColumns().size(); t++) {
            values.emplace_back(tuple.GetValue(&right_child_->GetOutputSchema(), t));
        }

        Tuple tmp_tuple = Tuple(values, &GetOutputSchema());
        tuples_.emplace_back(tmp_tuple);
      }

      *tuple = tuples_.back();
      tuples_.pop_back();
      return true;
    }
  }

  return false;
}

}  // namespace bustub
