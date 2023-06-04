#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> tmp = root_;

  for (char c : key) {
    if (tmp == nullptr) {
      return nullptr;
    }
    auto it = tmp->children_.find(c);
    if (it != tmp->children_.end()) {
      tmp = it->second;
    } else {
      return nullptr;
    }
  }

  const auto *node = dynamic_cast<const TrieNodeWithValue<T> *>(tmp.get());
  if (node == nullptr) {
    return nullptr;
  }
  return node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::vector<std::shared_ptr<const TrieNode>> vec;
  std::shared_ptr<T> shared_value = std::make_shared<T>(std::move(value));
  std::shared_ptr<const TrieNode> cur = root_;
  auto size = key.size();
  decltype(size) idx = 0;
  while (idx < size && cur) {
    char c = key[idx++];
    vec.emplace_back(cur);
    cur = cur->children_.find(c) != cur->children_.end() ? cur->children_.at(c) : nullptr;
  }

  std::shared_ptr<const TrieNodeWithValue<T>> leaf =
      cur ? std::make_shared<const TrieNodeWithValue<T>>(cur->children_, shared_value)
          : std::make_shared<const TrieNodeWithValue<T>>(shared_value);

  std::shared_ptr<const TrieNode> child_node = leaf;
  while (idx < size) {
    char c = key[--size];
    std::map<char, std::shared_ptr<const TrieNode>> children({{c, child_node}});
    child_node = std::make_shared<const TrieNode>(children);
  }

  cur = child_node;
  for (size_t i = vec.size() - 1; i < vec.size(); i--) {
    cur = std::shared_ptr<TrieNode>(vec[i]->Clone());
    const_cast<TrieNode *>(cur.get())->children_[key[i]] = child_node;
    child_node = cur;
  }

  return Trie(cur);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  std::vector<std::shared_ptr<const TrieNode>> vec;
  std::shared_ptr<const TrieNode> cur = root_;
  auto size = key.size();
  decltype(size) idx = 0;
  while (idx < size && cur) {
    char c = key[idx++];
    vec.emplace_back(cur);
    cur = cur->children_.find(c) != cur->children_.end() ? cur->children_.at(c) : nullptr;
  }

  if (!cur || !cur->is_value_node_) {
    return *this;
  }

  std::shared_ptr<const TrieNode> end_node =
      cur->children_.empty() ? nullptr : std::make_shared<const TrieNode>(cur->children_);

  cur = end_node;
  for (size_t i = vec.size() - 1; i < vec.size(); i--) {
    cur = std::shared_ptr<TrieNode>(vec[i]->Clone());
    const_cast<TrieNode *>(cur.get())->children_[key[i]] = end_node;
    end_node = cur;
  }

  return Trie(cur);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
