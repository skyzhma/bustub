//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct JointKey {
  /** The group-by values */
  Value value_;
  auto operator==(const JointKey &other) const -> bool {
    return value_.CompareEquals(other.value_) == CmpBool::CmpTrue;
  }
};
}


namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::JointKey> {
  auto operator()(const bustub::JointKey &joint_key) const -> std::size_t {
    size_t curr_hash = 0;
    if (!joint_key.value_.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&joint_key.value_));
    }
    
    return curr_hash;
  }
};

}  // namespace std


namespace bustub {

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class HashJoinTable {
 public:

  HashJoinTable() = default;

  void Insert(const JointKey &key, const Tuple &val) {
    if (ht_.count(key) == 0) {
      ht_.insert({key, std::vector<Tuple>{val}});
    } else {
      ht_[key].emplace_back(val);
    }
  }

  auto Get(const JointKey &key) -> std::vector<Tuple> {
    if (ht_.count(key) > 0) {
      return ht_[key];
    }
    return std::vector<Tuple>{};
  }

  auto Clear() {
    ht_.clear();
  }

 private:
  std::unordered_map<JointKey, std::vector<Tuple>> ht_{};
};


/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  HashJoinTable ht_[2];
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  
  std::vector<Tuple> tuples_;
};

}  // namespace bustub

