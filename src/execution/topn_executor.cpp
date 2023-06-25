#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  order_by_ = plan_->GetOrderBy();

  child_executor_->Init();

  Tuple tuple{};
  RID rid{};

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
    // if (i == tuples_.size()) {
    //     tuples_.emplace_back(tuple);
    // } else {
    //     tuples_.insert(tuples_.begin() + i, tuple);
    //     // for (size_t k = tuples_.size() - 1; k > i; k--) {
    //     //     tuples_[k] = tuples_[k-1];
    //     // }
    //     // tuples_[i] = tuple;
    // }

    if (tuples_.size() > plan_->GetN()) {
      tuples_.pop_back();
    }
  }
}

void TopNExecutor::Init() { num_ = 0; }

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (num_ == tuples_.size()) {
    return false;
  }
  *tuple = tuples_[num_++];
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };

}  // namespace bustub
