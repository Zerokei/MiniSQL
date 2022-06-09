#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType :: LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  next_page_id_ = INVALID_PAGE_ID;
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  int L = 0, R = GetSize() - 1, p = GetSize();
  while(L <= R) {
    int mid = (L + R) >> 1;
    int o = comparator(key, array_[mid].first);
    if(o < 0) p = mid, R = mid - 1;
    else if(o > 0) L = mid + 1;
    else return mid;
  }
  return p;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array_[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Arrange(BPlusTreeLeafPage *recipient) {
  int tot = GetSize() + recipient->GetSize();
  int new_size = (tot + 1) / 2;
  MappingType *r = recipient->GetData();
  if(GetSize() < new_size) {
    int c = new_size - GetSize();
    for(int i = 0; i < c; ++i) array_[GetSize() + i] = r[i];
    for(int i = c; i < recipient->GetSize(); ++i) r[i - c] = r[i];
  } else {
    int c = GetSize() - new_size;
    for(int i = recipient->GetSize() - 1; i >= 0; --i) r[i + c] = r[i];
    for(int i = 0; i < c; ++i) r[i] = array_[GetSize() + i];
  }
  SetSize(new_size);
  recipient->SetSize(tot - new_size);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  int L = 0, R = GetSize() - 1, p = GetSize();
  while(L <= R) {
    int mid = (L + R) >> 1;
    int o = comparator(key, array_[mid].first);
    if(o < 0) p = mid, R = mid - 1;
    else L = mid + 1;
  }
  for(int i = GetSize() - 1; i >= p; --i) array_[i + 1] = array_[i];
  array_[p] = make_pair(key, value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {
  
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {

}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  int L = 0, R = GetSize() - 1;
  while(L <= R) {
    int mid = (L + R) >> 1;
    int o = comparator(key, array_[mid].first);
    if(o < 0) R = mid - 1;
    else if(o > 0) L = mid + 1;
    else return value = array_[mid].second, true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator) {
  return 0;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {

}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {

}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {

}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {

}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {

}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;

template
class BPlusTreeLeafPage<GenericKey<128>, RowId, GenericComparator<128>>;

template
class BPlusTreeLeafPage<GenericKey<256>, RowId, GenericComparator<256>>;