#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  order_by_ = plan_->GetOrderBy();

  child_executor_->Init();

  Tuple tuple{};
  RID rid{};

  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [this](Tuple &tuple1, Tuple &tuple2) {
    for (const auto &[order_by_type, expr] : order_by_) {
      Value value1;
      Value value2;
      if (const auto expression = dynamic_cast<const ColumnValueExpression *>(expr.get()); expression != nullptr) {
        value1 = tuple1.GetValue(&plan_->OutputSchema(), expression->GetColIdx());
        value2 = tuple2.GetValue(&plan_->OutputSchema(), expression->GetColIdx());
      } else {
        const auto cal_expr = dynamic_cast<const ArithmeticExpression *>(expr.get());
        value1 = cal_expr->Evaluate(&tuple1, plan_->OutputSchema());
        value2 = cal_expr->Evaluate(&tuple2, plan_->OutputSchema());
      }

      auto flag = (order_by_type == OrderByType::DESC);
      if (value1.CompareEquals(value2) == CmpBool::CmpTrue) {
        continue;
      }
      if (value1.CompareGreaterThan(value2) == CmpBool::CmpTrue) {
        return flag;
      }
      return !flag;
    }

    return true;
  });
}

void SortExecutor::Init() {
  child_executor_->Init();
  index_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (index_ != tuples_.size()) {
    *tuple = tuples_[index_];
    index_++;
    return true;
  }

  return false;
}

}  // namespace bustub
