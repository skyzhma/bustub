#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &projection_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "Join with 2 children");

    const auto &left_plan = optimized_plan->children_[0];
    const auto &right_plan = optimized_plan->children_[1];

    const auto &left_child_schema = left_plan->OutputSchema();
    const auto &right_child_schema = right_plan->OutputSchema();
    const auto &projection_schema = projection_plan.OutputSchema();

    const auto &left_child_columns = left_child_schema.GetColumns();
    const auto &right_child_columns = right_child_schema.GetColumns();
    const auto &projection_columns = projection_schema.GetColumns();


  }





  return plan;
}

}  // namespace bustub
