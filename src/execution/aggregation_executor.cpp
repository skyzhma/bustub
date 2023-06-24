//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor{exec_ctx},
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  aggregate_finished_ = false;
  empty_ = true;
  child_->Init();
  aht_.Clear();
  Tuple tuple{};
  RID rid{};
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    empty_ = false;
  }

  aht_iterator_ = SimpleAggregationHashTable::Iterator(aht_.HtBegin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aggregate_finished_) {
    return false;
  }

  bool flag = true;
  // empty table
  if (aht_iterator_ == aht_.End()) {
    if (empty_) {
      if (plan_->GetGroupBys().capacity() > 0) {
        flag = false;
      } else {
        std::vector<Value> values{};
        values.reserve(GetOutputSchema().GetColumnCount());
        for (size_t i = 0; i < plan_->GetAggregateTypes().capacity(); i++) {
          if (plan_->GetAggregateTypes()[i] == AggregationType::CountStarAggregate) {
            values.emplace_back(ValueFactory::GetIntegerValue(0));
          } else {
            values.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
          }
        }

        *tuple = Tuple(values, &plan_->OutputSchema());
      }

    } else {
      flag = false;
    }

    aggregate_finished_ = true;
  } else {
    auto key = aht_iterator_.Key();
    auto value = aht_iterator_.Val();

    if (plan_->GetGroupBys().capacity() > 0) {
      key.group_bys_.insert(key.group_bys_.end(), value.aggregates_.begin(), value.aggregates_.end());
      *tuple = Tuple{key.group_bys_, &plan_->OutputSchema()};
    } else {
      *tuple = Tuple{value.aggregates_, &plan_->OutputSchema()};
    }

    ++aht_iterator_;
  }

  return flag;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
