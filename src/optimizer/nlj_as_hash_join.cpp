#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

void Process(const ComparisonExpression *expr, std::vector<AbstractExpressionRef> &left_exprs, std::vector<AbstractExpressionRef> &right_exprs) {
  
  for (const auto &child : expr->children_) {
    if (auto expr = dynamic_cast<const ColumnValueExpression *>(child.get()); expr->GetTupleIdx() == 0) {
      left_exprs.emplace_back(child);
    } else {
      right_exprs.emplace_back(child);
    }
  }

}


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

    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    
    if (const auto expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        
        std::vector<AbstractExpressionRef> left_exprs;
        std::vector<AbstractExpressionRef> right_exprs;
        Process(expr, left_exprs, right_exprs);
        // int t = 0;
        // for (const auto &child : expr->children_) {
        //   if (auto expr = dynamic_cast<const ColumnValueExpression *>(child.get()); expr->GetTupleIdx() == 0) {
        //     left_exprs.emplace_back(child);
        //   } else {
        //     t += 1;
        //     right_exprs.emplace_back(child);
        //   }
        // }
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), left_exprs,
                                                      right_exprs, nlj_plan.GetJoinType());
        
      }
    } else if (const auto expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {

        std::vector<AbstractExpressionRef> left_exprs;
        std::vector<AbstractExpressionRef> right_exprs;
        BUSTUB_ENSURE(expr->GetChildren().size() == 2, "NLJ should have exactly 2 children.");
        const auto children = expr->GetChildren();
        for (const auto &child : children) {
          if (const auto expr = dynamic_cast<const ComparisonExpression *>(child.get()); expr != nullptr) {
            if (expr->comp_type_ == ComparisonType::Equal) {
              Process(expr, left_exprs, right_exprs);
            }
          }
        }
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                              nlj_plan.GetRightPlan(), left_exprs,
                                              right_exprs, nlj_plan.GetJoinType());
      }
    }

  }

  return plan;
}

}  // namespace bustub
