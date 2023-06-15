//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
//   SetPageType(IndexPageType::LEAF_PAGE);
//   SetSize(1);
//   SetMaxSize(max_size);
//   next_page_id_ = INVALID_PAGE_ID;
// }

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindIndex(const KeyType &key, KeyComparator comp) -> int {
  int i = 0;
  int j = GetSize();
  while (i < j) {
    int t = (i + j) / 2;
    int res = comp(key, KeyAt(t));
    if (res > 0) {
      i = t + 1;
    } else {
      j = t;
    }
  }

  return i;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::LookUp(const KeyType &key, KeyComparator comp, ValueType &v) -> bool {
  int index = FindIndex(key, comp);
  if (index < GetSize() && comp(key, array_[index].first) == 0) {
    v = array_[index].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, KeyComparator comp, const ValueType &v) -> bool {
  int index = FindIndex(key, comp);

  // std::cout << "Inserting---" << index << " " << GetSize() << " " << GetMaxSize() << std::endl;
  if (index < GetSize() && comp(key, array_[index].first) == 0) {
    return false;
  }

  if (index < GetSize()) {
    for (int i = GetSize(); i > index; i--) {
      array_[i] = array_[i-1];
    }
  }

  array_[index].first = key;
  array_[index].second = v;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKey(const KeyType &key, KeyComparator comp) -> bool {
  for (int i = 0; i < GetSize(); i++) {
    if (comp(key, KeyAt(i)) == 0) {
      // find the key, remove it
      for (int j = i; j < GetSize(); j++) {
        array_[j].first = array_[j+1].first;
        array_[j].second = array_[j+1].second;
      }
      IncreaseSize(-1);
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveKey(int index) {
  for (int i = index; i < GetSize(); i++) {
    array_[i].first = array_[i+1].first;
    array_[i].second = array_[i+1].second;
  }
  IncreaseSize(-1);
}


template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
