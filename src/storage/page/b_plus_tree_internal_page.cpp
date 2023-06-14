//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
//   SetPageType(IndexPageType::INTERNAL_PAGE);
//   SetSize(2);
//   SetMaxSize(max_size);
// }
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
//   // replace with your own code
//   return array_[index].first;
// }

// INDEX_TEMPLATE_ARGUMENTS
// void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
//   array_[index].first = key;
// }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
// INDEX_TEMPLATE_ARGUMENTS
// auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const KeyType &key, KeyComparator comp) -> int {
  int i = 1;
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::LookUp(const KeyType &key, KeyComparator comp, ValueType &v) {
  int index = FindIndex(key, comp);

  if (index < GetSize() && comp(key, KeyAt(index)) == 0) {
    v = array_[index].second;
  } else {
    v = array_[index - 1].second;
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, KeyComparator comp, const ValueType &v) -> bool {
  int index = FindIndex(key, comp);
  if (index < GetSize() && comp(key, array_[index].first) == 0) {
    return false;
  }

  if (index < GetSize()) {
    for (int i = GetSize(); i >= index; i--) {
      array_[i + 1] = array_[i];
    }
  }

  // for (int i = GetSize(); i >= index; i--) {
  //   array_[i + 1] = array_[i];
  // }
  array_[index].first = key;
  array_[index].second = v;
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveKey(const KeyType &key, KeyComparator comp) {

    int index = FindIndex(key, comp);
    for (int i = index; i < GetSize() - 1; i++) {
      array_[i].first = array_[i+1].first;
      array_[i].second = array_[i+1].second;
    }
    IncreaseSize(-1);

}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
