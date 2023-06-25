#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  order_by_ = plan_->GetOrderBy();
  num_ = 0;
  tuples_.clear();

  Tuple tuple{};
  RID rid{};
  child_executor_->Init();

  while (child_executor_->Next(&tuple, &rid)) {
    // binary search
    size_t i = 0;
    size_t j = tuples_.size();
    while (i < j) {
      size_t k = (i + j) / 2;
      if (Comp(tuples_[k], tuple)) {
        i = k + 1;
      } else {
        j = k;
      }
    }

    tuples_.insert(tuples_.begin() + i, tuple);
    if (tuples_.size() > plan_->GetN()) {
      tuples_.pop_back();
    }
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (num_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[num_++];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };

}  // namespace bustub
