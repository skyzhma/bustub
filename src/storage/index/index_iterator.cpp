/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : bpm_(buffer_pool_manager), page_id_(page_id), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {}  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  auto guard = bpm_->FetchPageRead(page_id_);
  auto page = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return static_cast<bool>(page->GetNextPageId() == INVALID_PAGE_ID && index_ == page->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  auto guard = bpm_->FetchPageRead(page_id_);
  auto page = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  return page->GetPair(index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_ += 1;
  auto guard = bpm_->FetchPageRead(page_id_);
  auto page = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (index_ == page->GetSize()) {
    if (page->GetNextPageId() != INVALID_PAGE_ID) {
      index_ = 0;
      page_id_ = page->GetNextPageId();
    } else {
      bpm_ = nullptr;
      page_id_ = INVALID_PAGE_ID;
      index_ = -1;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
