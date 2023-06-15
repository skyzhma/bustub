#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
  root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  if (IsEmpty()) {
    std::cout << "GetValue return false 1" << std::endl;
    return false;
  }
  header_guard.Drop();

  FindLeafPage(key, ctx, false);

  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  // auto guard = bpm_->FetchPageBasic(leaf_page_id);
  auto page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  ValueType v;
  if (page->LookUp(key, comparator_, v)) {
    result->push_back(v);
    return true;
  }
  std::cout << "GetValue return false 2" << std::endl;
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);

  if (IsEmpty()) {
    // If the root page is null, create a new root page
    // auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto root_guard = bpm_->NewPageGuarded(&root_page_id_);
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id_;
    auto root_page = root_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    // root_page->SetPageType(IndexPageType::LEAF_PAGE);
    root_page->Init(leaf_max_size_);
  }

  // record the root page id
  ctx.root_page_id_ = root_page_id_;
  // ctx.header_page_ = std::move(header_guard);
  // header_guard.Drop();

  // Get the leaf page and insert the key-value to the leaf page
  // If insert fail, then return false
  FindLeafPage(key, ctx, true, true);

  bool flag = (ctx.write_set_.front().PageId() != root_page_id_);
  if (flag) {
    header_guard.Drop();
  }

  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  auto leaf_page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  if (!leaf_page->Insert(key, comparator_, value)) {
    // Relase all the locks in ctx
    ctx.ReleaseAll();
    return false;
  }

  // Insert succeed. Check if need to split the leaf node
  if (leaf_page->GetSize() != leaf_max_size_) {
    // Relase all the locks in ctx
    ctx.ReleaseAll();
    return true;
  }

  page_id_t split_page_id;
  auto split_page_guard = bpm_->NewPageGuarded(&split_page_id);
  auto split_page = split_page_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  split_page->Init(leaf_max_size_);

  // Copy data
  int index = 0;
  for (int i = leaf_max_size_ / 2; i < leaf_max_size_; i++) {
    split_page->SetPair(index++, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
  }

  // Set size
  leaf_page->SetSize(leaf_max_size_ / 2);
  split_page->SetSize(leaf_max_size_ - leaf_max_size_ / 2);

  // Set next page id
  split_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(split_page_id);

  // Insert into parent

  InsertParent(guard.PageId(), split_page_guard.PageId(), split_page->KeyAt(0), ctx);

  // Relase all the locks in ctx

  if (!flag) {
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id_;
  }

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertParent(page_id_t left_page_id, page_id_t right_page_id, KeyType key, Context &ctx) {
  if (left_page_id == root_page_id_) {
    // auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    // auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    // header_page->root_page_id_ = root_page_id_;

    auto guard = bpm_->NewPageGuarded(&root_page_id_);
    auto page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    page->Init(internal_max_size_);
    // page->SetPageType(IndexPageType::INTERNAL_PAGE);
    page->SetKeyAt(1, key);
    page->SetValueAt(0, left_page_id);
    page->SetValueAt(1, right_page_id);
    page->SetSize(2);
    return;
  }

  // Get the parent
  // should be deconstructed after leaving this function
  auto left_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  // check the size of the parent
  auto parent_page = left_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  if (parent_page->GetSize() < internal_max_size_) {
    // Insert into the Internal page
    parent_page->Insert(key, comparator_, right_page_id);

    // left_guard will deconstructed automatically
    return;
  }

  // copy data to a big array
  int j = parent_page->FindIndex(key, comparator_);
  std::pair<KeyType, page_id_t> array[internal_max_size_ + 1];
  int index = 0;
  for (int i = 0; i < j; i++) {
    array[index].first = parent_page->KeyAt(i);
    array[index++].second = parent_page->ValueAt(i);
  }
  array[index].first = key;
  array[index++].second = right_page_id;

  for (int i = j; i < parent_page->GetSize(); i++) {
    array[index].first = parent_page->KeyAt(i);
    array[index++].second = parent_page->ValueAt(i);
  }

  // Split a new parent page
  page_id_t parent_right_page_id;
  auto right_guard = bpm_->NewPageGuarded(&parent_right_page_id);
  auto parent_right_page = right_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  parent_right_page->Init(internal_max_size_);

  for (int i = 0; i < parent_page->GetMinSize(); i++) {
    parent_page->SetPair(i, array[i].first, array[i].second);
  }
  parent_page->SetSize(parent_page->GetMinSize());

  // the key transfer to the upper parent
  parent_right_page->SetValueAt(0, array[parent_page->GetMinSize()].second);

  // copy data to the right page
  index = 1;
  for (int i = parent_page->GetMinSize() + 1; i <= internal_max_size_; i++) {
    parent_right_page->SetPair(index++, array[i].first, array[i].second);
  }

  // Update Size
  parent_right_page->SetSize(internal_max_size_ + 1 - parent_page->GetMinSize());

  // Iteratively update parent
  InsertParent(left_guard.PageId(), right_guard.PageId(), array[parent_page->GetMinSize()].first, ctx);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);

  if (IsEmpty()) {
    return ;
  }

  FindLeafPage(key, ctx, true, false);

  bool flag = (ctx.write_set_.front().PageId() == root_page_id_);
  if (!flag) {
    header_guard.Drop();
  }

  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  // The key doesn't exist
  if (!page->RemoveKey(key, comparator_)) {
    ctx.ReleaseAll();
    return ;
  }

  // The leaf page has enough size or this is a root node
  if (page->GetSize() >= page->GetMinSize()) {
    ctx.ReleaseAll();
    return ;
  }

  // header_guard = bpm_->FetchPageWrite(header_page_id_);
  // Current tree is null
  if (guard.PageId() == root_page_id_) {
    if (page->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = INVALID_PAGE_ID;
    }
    return ;
  }

  // header_guard.Drop();

  // Need to colease or merge the sibiling node
  // Get its parent
  auto parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  auto parent_page = parent_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  int index = parent_page->FindIndex(key, comparator_);
  
  // Index : the first index that is greater than current key
   
  if (index == parent_page->GetSize() || comparator_(key, parent_page->KeyAt(index)) != 0) {
    index--;
  }

  // first borrow the key from the right sibling
  if (index + 1 < parent_page->GetSize() ) {
    WritePageGuard silbling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index+1));
    auto silbling_page = silbling_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    if (silbling_page->GetSize() >= silbling_page->GetMinSize() + 1) {
      // can borrow the key from the silbling
      page->SetPair(page->GetSize(), silbling_page->KeyAt(0), silbling_page->ValueAt(0));
      page->IncreaseSize(1);
      silbling_page->RemoveKey(0);

      // Set the key of silbling_page
      parent_page->SetKeyAt(index+1, silbling_page->KeyAt(0));

      // If the key to be deleted is the first key of page, reset the key of the parent page
      if (index >= 1 && comparator_(key, parent_page->KeyAt(index))) {
        parent_page->SetKeyAt(index, page->KeyAt(0));
      }

      // all good, release the lock along this path 
      ctx.ReleaseAll();
      
    } else {
      // merge the silbling page

      for (int i = page->GetSize(), j = 0; j < silbling_page->GetSize(); j++, i++) {
        page->SetPair(i, silbling_page->KeyAt(j), silbling_page->ValueAt(j));
      }

      page->SetSize(page->GetSize() + silbling_page->GetSize());

      page->SetNextPageId(silbling_page->GetNextPageId());

      // delete the sibling page
      bpm_->DeletePage(silbling_guard.PageId());

      // release the guard
      silbling_guard.Drop();

      // page_id_t parent_page_id = parent_guard.PageId();
      // ctx.write_set_.push_back(std::move(parent_guard));

      // The parent are beging locked, no need to push the guard back
      KeyType delete_key = parent_page->KeyAt(index + 1);

      RemoveParent(std::move(parent_guard), delete_key, ctx);

      // RemoveParent(std::move(parent_guard), page->KeyAt(page->GetSize()-1), ctx);
    }
  } else {
    // borrow the key from the left sibling
    // index of left sibling in parent page is index - 1
    WritePageGuard silbling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index-1));
    auto silbling_page = silbling_guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    
    if (silbling_page->GetSize() >= silbling_page->GetMinSize() + 1) {

      page->Insert(silbling_page->KeyAt(silbling_page->GetSize()-1),
                  comparator_,
                  silbling_page->ValueAt(silbling_page->GetSize()-1)
                  );
      silbling_page->RemoveKey(silbling_page->GetSize()-1);

      parent_page->SetKeyAt(index, page->KeyAt(0));

      // all good, release all the lock
      ctx.ReleaseAll();
      
    } else {

      // merge the left silbling
      for (int i = 0, j = silbling_page->GetSize(); i < page->GetSize(); i++, j++) {
        silbling_page->SetPair(j, page->KeyAt(i), page->ValueAt(i));
      }

      silbling_page->SetSize(silbling_page->GetSize() + page->GetSize());

      KeyType delete_key = parent_page->KeyAt(index);

      silbling_page->SetNextPageId(page->GetNextPageId());

      // delete the page
      bpm_->DeletePage(guard.PageId());
      
      // delete the page, release the lock
      guard.Drop();

      // release the silbling_guard
      silbling_guard.Drop();

      // Delete the key in parent page
      RemoveParent(std::move(parent_guard), delete_key, ctx);

    }

  }

  if (flag) {
    auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id_;
  }

}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveParent(WritePageGuard &&guard, const KeyType &key, Context &ctx) {

  // WritePageGuard guard = std::move(ctx.write_set_.back());
  // ctx.write_set_.pop_back();

  // WritePageGuard guard = bpm_->FetchPageWrite(page_id);
  auto page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  page->RemoveKey(key, comparator_);

  // Keep 
  if (page->GetSize() >= page->GetMinSize()) {
    ctx.ReleaseAll();
    return ;
  }

  // If this is a root page
  if (guard.PageId() == root_page_id_) {
    if (page->GetSize() == 1) {
      root_page_id_ = page->ValueAt(0);
      // WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
      // auto header_page = header_guard.AsMut<BPlusTreeHeaderPage>();
      // header_page->root_page_id_ = root_page_id_;
      bpm_->DeletePage(guard.PageId());
    }
    guard.Drop();
    return ;
  }

  WritePageGuard parent_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto parent_page = parent_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

  // The first index that's greate than the key in parent page
  int index = parent_page->FindIndex(key, comparator_);
  if (index == parent_page->GetSize() || comparator_(parent_page->KeyAt(index), key) != 0) {
    index--;
  }

  if (index + 1 < parent_page->GetSize()) {
    WritePageGuard silbling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index+1));
    auto silbling_page = silbling_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    // borrow the right page    
    if (silbling_page->GetSize() >= silbling_page->GetMinSize() + 1) {

      page->SetKeyAt(page->GetSize(), parent_page->KeyAt(index + 1));
      page->SetValueAt(page->GetSize(), silbling_page->ValueAt(0));
      page->IncreaseSize(1);

      // silbling page remove first key
      parent_page->SetKeyAt(index + 1, silbling_page->KeyAt(1));
      for (int i = 1; i < silbling_page->GetSize(); i++) {
        silbling_page->SetValueAt(i-1, silbling_page->ValueAt(i));
        if (i >= 2 ) {
          silbling_page->SetKeyAt(i-1, silbling_page->KeyAt(i));
        }
      }
      silbling_page->IncreaseSize(-1);

      // all good, release all the key
      ctx.ReleaseAll();

    } else {
      // merge the right

      page->SetKeyAt(page->GetSize(), parent_page->KeyAt(index + 1));
      for (int i = page->GetSize(), j = 0; j < silbling_page->GetSize(); i++, j++) {
        page->SetValueAt(i, silbling_page->ValueAt(j));
        if (j >= 1) {
          page->SetKeyAt(i, silbling_page->KeyAt(j));
        }
      }
      page->SetSize(page->GetSize() + silbling_page->GetSize());

      bpm_->DeletePage(silbling_guard.PageId());
      silbling_guard.Drop();

      KeyType delete_key = parent_page->KeyAt(index + 1);

      RemoveParent(std::move(parent_guard), delete_key, ctx);

    }

  } else {

    WritePageGuard silbling_guard = bpm_->FetchPageWrite(parent_page->ValueAt(index-1));
    auto silbling_page = silbling_guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

    if (silbling_page->GetSize() >= silbling_page->GetMinSize() + 1) {

        for (int i = page->GetSize(); i >= 1; i--) {
          page->SetKeyAt(i, page->KeyAt(i-1));
          page->SetValueAt(i, page->ValueAt(i-1));
        }

        // borrow the last key-value pair of the left silblibg page
        page->SetValueAt(0, silbling_page->ValueAt(silbling_page->GetSize()-1));
        page->SetKeyAt(1, parent_page->KeyAt(index));
        page->IncreaseSize(1);
        parent_page->SetKeyAt(index, silbling_page->KeyAt(silbling_page->GetSize()-1));

        silbling_page->IncreaseSize(-1);
        ctx.ReleaseAll();

    } else {

        // merge the left silbling page

        silbling_page->SetKeyAt(silbling_page->GetSize(), parent_page->KeyAt(index));
        for (int i = silbling_page->GetSize(), j = 0; j < page->GetSize(); j++, i++) {
          silbling_page->SetValueAt(i, page->ValueAt(j));
          if (j >= 1) {
            silbling_page->SetKeyAt(i, page->KeyAt(j));
          }
        }
        
        silbling_page->SetSize(silbling_page->GetSize() + page->GetSize());

        // delete the page
        bpm_->DeletePage(guard.PageId());

        // can drop or not
        guard.Drop();
        silbling_guard.Drop();

        KeyType delete_key = parent_page->KeyAt(index);

        RemoveParent(std::move(parent_guard), delete_key, ctx);

    }

  }



}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, -1);
  }

  auto guard = bpm_->FetchPageRead(root_page_id_);

  while (!guard.As<BPlusTreePage>()->IsLeafPage()) {
    auto page = guard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto next_page_id = page->ValueAt(0);
    auto child_guard = bpm_->FetchPageRead(next_page_id);
    guard = std::move(child_guard);
  }

  return INDEXITERATOR_TYPE(bpm_, guard.PageId(), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {

  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  if (root_page_id_ == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, -1);
  }

  Context ctx;
  FindLeafPage(key, ctx, false);
  WritePageGuard guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();

  if (guard.PageId() == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, -1);
  }

  auto page = guard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  for (int i = 0; i < page->GetSize(); i++) {
    if (comparator_(key, page->KeyAt(i)) == 0) {
      page_id_t id = guard.PageId();
      guard.Drop();
      return INDEXITERATOR_TYPE(bpm_, id, i);
    }
  }

  return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, -1);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Context &ctx, bool record, bool insert) {

  auto guard = bpm_->FetchPageWrite(root_page_id_);
  while (!guard.AsMut<BPlusTreePage>()->IsLeafPage()) {
    auto page = guard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

    // For insertion
    if (insert && record &&  page->GetSize() < page->GetMaxSize()) {
      // It's save to release all the lock before this node
      ctx.ReleaseAll();
    }

    if (!insert && record && page->GetSize() > page->GetMinSize()) {
      ctx.ReleaseAll();
    }

    if (record) {
      ctx.write_set_.push_back(std::move(guard));
    }

    page_id_t t;
    page->LookUp(key, comparator_, t);

    auto child_guard = bpm_->FetchPageWrite(t);
    // execute the deconstructor function of current guard (unpin the page)
    guard = std::move(child_guard);
  }

  ctx.write_set_.push_back(std::move(guard));

}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
