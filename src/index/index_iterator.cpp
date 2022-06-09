#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/b_plus_tree.h"
#include "index/index_iterator.h"
#include "buffer/buffer_pool_manager.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(BPlusTree<KeyType, ValueType, KeyComparator> *tree, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf, int index)
  : tree_(tree), leaf_(leaf), index_(index) {
}


INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if(leaf_ != nullptr) tree_->buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  return leaf_->GetData()[index_];
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if(leaf_ == nullptr) return *this;
  if(index_ < leaf_->GetSize() - 1){
    index_++;
  } else {
    page_id_t next = leaf_->GetNextPageId();
    tree_->buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), true);
    index_ = 0;
    if(next == INVALID_PAGE_ID) leaf_ = nullptr;
    else leaf_ = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> *>(tree_->buffer_pool_manager_->FetchPage(next)->GetData());
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  return tree_ == itr.tree_ && leaf_ == itr.leaf_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;

template
class IndexIterator<GenericKey<128>, RowId, GenericComparator<128>>;

template
class IndexIterator<GenericKey<256>, RowId, GenericComparator<256>>;
