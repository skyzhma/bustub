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

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
            aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())) {    
    
        aht_iterator_ = aht_.Begin();

        Tuple *tuple;
        RID *rid;

        while (child->Next(tuple, rid)) {
            aht_.InsertCombine(MakeAggregateKey(tuple), MakeAggregateValue(tuple));
        }

    }

void AggregationExecutor::Init() {}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {

    if (aht_iterator_ == aht_.End()) {
        return false;
    }

    

 }

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
