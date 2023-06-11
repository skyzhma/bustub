//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <string>

#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 12
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // Deleted to disallow initialization
  BPlusTreeInternalPage() = delete;
  BPlusTreeInternalPage(const BPlusTreeInternalPage &other) = delete;

  /**
   * Writes the necessary header information to a newly created page, must be called after
   * the creation of a new page to make a valid BPlusTreeInternalPage
   * @param max_size Maximal size of the page
   */
  void Init(int max_size = INTERNAL_PAGE_SIZE) {

    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(0);
    SetMaxSize(max_size);
  }

  /**
   * @param index The index of the key to get. Index must be non-zero.
   * @return Key at index
   */
  auto KeyAt(int index) const -> KeyType {
    return array_[index].first;
  }

  /**
   *
   * @param index The index of the key to set. Index must be non-zero.
   * @param key The new value for key
   */
  void SetKeyAt(int index, const KeyType &key) {
    array_[index].first = key;
  }

  /**
   *
   * @param value the value to search for
   */
  auto ValueIndex(const page_id_t &value) const -> int;

  /**
   *
   * @param index the index
   * @return the value at the index
   */
  auto ValueAt(int index) const -> page_id_t {
    return array_[index].second;
  }

  /**
   * @brief For test only, return a string representing all keys in
   * this internal page, formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    // first key of internal page is always invalid
    for (int i = 1; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

  auto FindIndex(const KeyType &key, KeyComparator comp) -> int {
    
    int i = 1;
    int j = GetSize();
    while (i < j) {
      int t = (i + j) / 2;
      int res = comp(KeyAt(t), key);
      if (res > 0) {
        i = t + 1;
      } else if (res < 0) {
        j = t;
      } else {
        return t;
      }
    }

    return i;
  }

  void LookUp(const KeyType &key, KeyComparator comp, page_id_t &v) {
    int index = FindIndex(key, comp);
    if (index == 1) {
      if (comp(key, KeyAt(1)) == 0) {
        v = array_[index].second;
      } else {
        v = array_[0].second;
      }
    } else {
      v = array_[index].second;
    }
  }

  auto Insert(const KeyType &key, KeyComparator comp, const page_id_t &v) -> bool {
    int index = FindIndex(key, comp);
    if (index < GetSize() && comp(key, array_[index].first) == 0) {
      return false;
    }
    for (int i = GetSize(); i >= index; i--) {
      array_[i+1]  = array_[i];
    }
    array_[index].first = key;
    array_[index].second = v;
    IncreaseSize(1);
    return true;
  }

  auto SetValueAt(int index, const page_id_t &v) {
    array_[index].second = v;
  }

  auto SetPair(int index, KeyType k, page_id_t v) {
    array_[index].first = k;
    array_[index].second = v;
  }

 private:
  // Flexible array member for page data.
  std::pair<KeyType, page_id_t> array_[0];
  // MappingType array_[0];
};
}  // namespace bustub
