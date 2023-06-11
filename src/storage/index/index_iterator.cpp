/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t page_id, int index)
    : bpm_(buffer_pool_manager), page_id_(page_id), index_(index), page_(nullptr) {
  if (page_id != INVALID_PAGE_ID) {
    guard_ = bpm_->FetchPageRead(page_id);
    page_ = guard_.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() { guard_.Drop(); }  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  if (page_id_ == INVALID_PAGE_ID) {
    return true;
  }
  if (page_->GetNextPageId() == INVALID_PAGE_ID && index_ == page_->GetMaxSize()) {
    return true;
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return page_->GetPair(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_ += 1;
  if (index_ == page_->GetSize()) {
    if (page_->GetNextPageId() != INVALID_PAGE_ID) {
      index_ = 0;
      auto child_guard = bpm_->FetchPageRead(page_->GetNextPageId());
      guard_ = std::move(child_guard);
    } else {
      guard_.Drop();
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
