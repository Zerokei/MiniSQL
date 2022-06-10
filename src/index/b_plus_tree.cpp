#include <string>
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include "common/config.h"

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  auto root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  root_page_id_ = INVALID_PAGE_ID;
  root_page->GetRootId(index_id, &root_page_id_);
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}


INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  if(IsEmpty()) return ;
  auto root_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  DestroyDown(root_tree_page);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  buffer_pool_manager_->DeletePage(root_page_id_);
  root_page_id_ = INVALID_PAGE_ID;
  UpdateRootPageId(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DestroyDown(BPlusTreePage *cur_tree_page) {
  if(cur_tree_page->IsLeafPage()) return ;
  auto internal_page = reinterpret_cast<InternalPage *>(cur_tree_page);
  for(int i = 0; i < internal_page->GetSize(); ++i) {
    auto nxt_page_id = internal_page->GetData()[i].second;
    auto nxt_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(nxt_page_id)->GetData());
    DestroyDown(nxt_tree_page);
    buffer_pool_manager_->UnpinPage(nxt_page_id, true);
    buffer_pool_manager_->DeletePage(nxt_page_id);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  auto p = FindLeafPage(key);
  if(p == nullptr) return false;
  ValueType value;
  bool find = reinterpret_cast<LeafPage *>(p->GetData())->Lookup(key, value, comparator_);
  if(find) result.push_back(value);
  buffer_pool_manager_->UnpinPage(p->GetPageId(),false);
  return find;
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
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  auto root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto root_tree_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  bool exist = false;
  KeyType new_key;
  auto new_page = InsertDown(root_tree_page, key, value, new_key, exist);
  if(exist || new_page == nullptr) {
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return !exist;
  }
  page_id_t new_root_page_id;
  auto new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(new_root_page_id)->GetData());
  new_root->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
  new_root->SetSize(2);
  new_root->GetData()[0] = make_pair(FirstKey(root_tree_page), root_tree_page->GetPageId());
  new_root->GetData()[1] = make_pair(FirstKey(new_page), new_page->GetPageId());
  root_tree_page->SetParentPageId(new_root_page_id);
  new_page->SetParentPageId(new_root_page_id);
  buffer_pool_manager_->UnpinPage(new_root_page_id, true);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  root_page_id_ = new_root_page_id;
  UpdateRootPageId(0);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
KeyType BPLUSTREE_TYPE::FirstKey(BPlusTreePage *p) {
  if(p->IsLeafPage()) return reinterpret_cast<LeafPage *>(p)->GetData()[0].first;
  else return reinterpret_cast<InternalPage *>(p)->GetData()[0].first;
}
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::InsertDown(BPlusTreePage *cur_tree_page,const KeyType &key, const ValueType &value, KeyType &min_key, bool &exist){
  if(cur_tree_page->IsLeafPage()){
    auto cur_leaf_page = reinterpret_cast<LeafPage *>(cur_tree_page);
    ValueType temp;
    auto cur_data = cur_leaf_page->GetData();
    if(cur_leaf_page->Lookup(key, temp, comparator_)) {
      return min_key = cur_data[0].first, exist = true, nullptr;
    }
    int pos = cur_leaf_page->KeyIndex(key, comparator_);
    if(cur_leaf_page->GetSize() < cur_leaf_page->GetMaxSize()) {
      cur_leaf_page->Insert(key, value, comparator_);
      return min_key = cur_data[0].first, nullptr;
    }
    page_id_t ext_page_id;
    auto ext_leaf_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(ext_page_id)->GetData());
    ext_leaf_page->Init(ext_page_id, cur_leaf_page->GetParentPageId(), leaf_max_size_);
    ext_leaf_page->SetSize(leaf_max_size_ + 1 - (leaf_max_size_ / 2));
    ext_leaf_page->SetNextPageId(cur_leaf_page->GetNextPageId());
    cur_leaf_page->SetSize(leaf_max_size_ / 2);
    cur_leaf_page->SetNextPageId(ext_page_id);
    auto ext_data = ext_leaf_page->GetData();
    for(int i = leaf_max_size_; i >= 0 ;i --){
      pair<KeyType, ValueType> cur_pair;
      if(i < pos) cur_pair = cur_data[i];
      else if(i == pos) cur_pair = make_pair(key, value);
      else cur_pair = cur_data[i - 1];
      if(i < internal_max_size_ / 2) cur_data[i] = cur_pair;
      else ext_data[i - internal_max_size_ / 2] = cur_pair;
    }
    return min_key = cur_data[0].first, ext_leaf_page;
  }
  page_id_t tar_page_id;
  auto cur_internal_page = reinterpret_cast<InternalPage * >(cur_tree_page);
  auto cur_data = cur_internal_page->GetData();
  tar_page_id = cur_internal_page->Lookup(key, comparator_);
  int tar_page_index = cur_internal_page->ValueIndex(tar_page_id);

  auto tar_page = buffer_pool_manager_->FetchPage(tar_page_id);
  
  auto tar_tree_page = reinterpret_cast<BPlusTreePage *>(tar_page->GetData());


  auto new_page = InsertDown(tar_tree_page, key, value, min_key, exist);

  cur_data[tar_page_index].first = min_key;

  if(new_page == nullptr) {
      buffer_pool_manager_->UnpinPage(tar_page_id, true);
      return min_key = cur_data[0].first, nullptr;
  }
  page_id_t new_page_id = new_page->GetPageId();
  
  KeyType new_key = FirstKey(new_page);
  
  if(cur_internal_page->GetSize() < internal_max_size_) {
    cur_internal_page->InsertNodeAfter(tar_page_id, new_key, new_page_id);
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    buffer_pool_manager_->UnpinPage(tar_page_id, true);
    return min_key = cur_data[0].first, nullptr;
  }
  
  page_id_t ext_page_id;
  
  auto ext_internal_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(ext_page_id)->GetData());
  auto ext_data = ext_internal_page->GetData();
  ext_internal_page->Init(ext_page_id, cur_internal_page->GetParentPageId(), internal_max_size_);
  ext_internal_page->SetSize(internal_max_size_ + 1 - (internal_max_size_ / 2));
  cur_internal_page->SetSize(internal_max_size_ / 2);
  for(int i = internal_max_size_; i >= 0 ;i --){
    pair<KeyType, page_id_t> cur_pair;
    if(i <= tar_page_index) cur_pair = cur_data[i];
    else if(i == tar_page_index + 1) cur_pair = make_pair(new_key, new_page_id);
    else cur_pair = cur_data[i - 1];
    if(i < internal_max_size_ / 2) cur_data[i] = cur_pair;
    else ext_data[i - internal_max_size_/2] = cur_pair;
  }
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(tar_page_id, true);
  return min_key = cur_data[0].first, ext_internal_page;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id;
  Page *p = buffer_pool_manager_->NewPage(root_page_id);
  LeafPage * leaf_page = reinterpret_cast<LeafPage *>(p->GetData());
  leaf_page->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);
  root_page_id_ = root_page_id;
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  return nullptr;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if(IsEmpty()) return ;
  auto root_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  RemoveDown(root_tree_page, key);
  if(!root_tree_page->IsLeafPage() && root_tree_page->GetSize() == 1) {
    auto p = reinterpret_cast<InternalPage *>(root_tree_page);
    auto new_root_page_id = p->GetData()[0].second;
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = new_root_page_id;
    root_tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    root_tree_page->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
    UpdateRootPageId(0);
  } else if(root_tree_page->IsLeafPage() && root_tree_page->GetSize() == 0) {
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(-1);
  } else {
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
KeyType BPLUSTREE_TYPE::RemoveDown(BPlusTreePage *cur_tree_page, const KeyType &key) {
  if(cur_tree_page->IsLeafPage()) {
    auto cur_leaf_page = reinterpret_cast<LeafPage *>(cur_tree_page);
    auto cur_data = cur_leaf_page->GetData();
    int cur_size = cur_leaf_page->GetSize();
    ValueType temp;
    if(cur_leaf_page->Lookup(key, temp, comparator_)) {
      int tar_row_index = cur_leaf_page->KeyIndex(key, comparator_);
      for(int i = tar_row_index; i < cur_size - 1; ++i) 
        cur_data[i] = cur_data[i + 1];
      cur_leaf_page->IncreaseSize(-1);
    }
    return cur_data[0].first;
  }
  page_id_t tar_page_id;
  auto cur_internal_page = reinterpret_cast<InternalPage *>(cur_tree_page);
  auto cur_data = cur_internal_page->GetData();
  tar_page_id = cur_internal_page->Lookup(key, comparator_);
  int tar_page_index = cur_internal_page->ValueIndex(tar_page_id);
  auto tar_page = buffer_pool_manager_->FetchPage(tar_page_id);
  auto tar_tree_page = reinterpret_cast<BPlusTreePage *>(tar_page->GetData());

  cur_data[tar_page_index].first = RemoveDown(tar_tree_page, key);

  auto remain_size = tar_tree_page->GetMaxSize() - tar_tree_page->GetSize();
  buffer_pool_manager_->UnpinPage(tar_page_id, true);
  if(tar_tree_page->GetSize() >= tar_tree_page->GetMinSize()) return cur_data[0].first;

  int left_index = -1;
  if(tar_page_index > 0) {
    auto sib_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[tar_page_index - 1].second);
    auto sib_tree_page = reinterpret_cast<BPlusTreePage *>(sib_page->GetData());
    if(sib_tree_page->GetSize() <= remain_size) left_index = tar_page_index - 1;
    buffer_pool_manager_->UnpinPage(sib_page->GetPageId(),false);
  }
  if(left_index == -1 && tar_page_index < cur_internal_page->GetSize() - 1) {
    auto sib_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[tar_page_index + 1].second);
    auto sib_tree_page = reinterpret_cast<BPlusTreePage *>(sib_page->GetData());
    if(sib_tree_page->GetSize() <= remain_size) left_index = tar_page_index;
    buffer_pool_manager_->UnpinPage(sib_page->GetPageId(),false);
  }
  if(left_index != -1) {
    auto left_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[left_index].second);
    auto left_tree_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
    auto right_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[left_index + 1].second);
    auto right_tree_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    char *src, *dest;
    size_t size;
    if(left_tree_page->IsLeafPage()){
      auto left_leaf = reinterpret_cast<LeafPage *>(left_tree_page);
      auto right_leaf = reinterpret_cast<LeafPage *>(right_tree_page);
      src = reinterpret_cast<char *>(right_leaf->GetData() );
      dest = reinterpret_cast<char *>(left_leaf->GetData() + left_leaf->GetSize());
      size = sizeof(pair<KeyType, ValueType>) * right_leaf->GetSize();
      left_leaf->SetNextPageId(right_leaf->GetNextPageId());
      left_leaf->IncreaseSize(right_leaf->GetSize());
    } else {
      auto left_leaf = reinterpret_cast<InternalPage *>(left_tree_page);
      auto right_leaf = reinterpret_cast<InternalPage *>(right_tree_page);
      src = reinterpret_cast<char *>(right_leaf->GetData() );
      dest = reinterpret_cast<char *>(left_leaf->GetData() + left_leaf->GetSize());
      size = sizeof(pair<KeyType, page_id_t>) * right_leaf->GetSize();
      left_leaf->IncreaseSize(right_leaf->GetSize());
    }
    memcpy(dest, src, size);
    buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(right_page->GetPageId());

    for(int i = left_index + 1;i < cur_internal_page->GetSize() - 1;i++) 
        cur_data[i] = cur_data[i + 1];
    cur_internal_page->IncreaseSize(-1);
    return cur_data[0].first;
  }
  if(tar_page_index > 0) {
    auto left_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[tar_page_index - 1].second);
    auto left_tree_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
    auto right_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[tar_page_index].second);
    auto right_tree_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    if(left_tree_page->IsLeafPage()) {
      auto left_leaf = reinterpret_cast<LeafPage *>(left_tree_page);
      auto right_leaf = reinterpret_cast<LeafPage *>(right_tree_page);
      for(int i = right_leaf->GetSize(); i > 0; --i) right_leaf->GetData()[i] = right_leaf->GetData()[i - 1];
      right_leaf->IncreaseSize(1);
      right_leaf->GetData()[0] = left_leaf->GetData()[left_leaf->GetSize() - 1];
      left_leaf->IncreaseSize(-1);
      cur_data[tar_page_index].first = right_leaf->GetData()[0].first;
    } else {
      auto left_leaf = reinterpret_cast<InternalPage *>(left_tree_page);
      auto right_leaf = reinterpret_cast<InternalPage *>(right_tree_page);
      for(int i = right_leaf->GetSize(); i > 0; --i) right_leaf->GetData()[i] = right_leaf->GetData()[i - 1];
      right_leaf->IncreaseSize(1);
      right_leaf->GetData()[0] = left_leaf->GetData()[left_leaf->GetSize() - 1];
      left_leaf->IncreaseSize(-1);
      cur_data[tar_page_index].first = right_leaf->GetData()[0].first;
    }
  } else {
    auto left_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[tar_page_index].second);
    auto left_tree_page = reinterpret_cast<BPlusTreePage *>(left_page->GetData());
    auto right_page = buffer_pool_manager_->FetchPage(cur_internal_page->GetData()[tar_page_index + 1].second);
    auto right_tree_page = reinterpret_cast<BPlusTreePage *>(right_page->GetData());
    if(left_tree_page->IsLeafPage()) {
      auto left_leaf = reinterpret_cast<LeafPage *>(left_tree_page);
      auto right_leaf = reinterpret_cast<LeafPage *>(right_tree_page);
      left_leaf->IncreaseSize(1);
      left_leaf->GetData()[left_leaf->GetSize() - 1] = right_leaf->GetData()[0];
      right_leaf->IncreaseSize(-1);
      for(int i = 0; i < right_leaf->GetSize(); ++i) right_leaf->GetData()[i] = right_leaf->GetData()[i + 1];
      cur_data[tar_page_index + 1].first = right_leaf->GetData()[0].first;
    } else {
      auto left_leaf = reinterpret_cast<InternalPage *>(left_tree_page);
      auto right_leaf = reinterpret_cast<InternalPage *>(right_tree_page);
      left_leaf->IncreaseSize(1);
      left_leaf->GetData()[left_leaf->GetSize() - 1] = right_leaf->GetData()[0];
      right_leaf->IncreaseSize(-1);
      for(int i = 0; i < right_leaf->GetSize(); ++i) right_leaf->GetData()[i] = right_leaf->GetData()[i + 1];
      cur_data[tar_page_index + 1].first = right_leaf->GetData()[0].first;
    }
  }
  return cur_data[0].first;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  if(IsEmpty()) return End();
  page_id_t now = root_page_id_;
  Page *p = buffer_pool_manager_->FetchPage(now);
  while(!reinterpret_cast<BPlusTreePage *>(p->GetData())->IsLeafPage()) {
    page_id_t nxt = reinterpret_cast<InternalPage *>(p->GetData())->GetData()[0].second;
    buffer_pool_manager_->UnpinPage(now, false);
    now = nxt;
    p = buffer_pool_manager_->FetchPage(now);
  }
  return INDEXITERATOR_TYPE(this, reinterpret_cast<LeafPage *>(p), 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto p = FindLeafPage(key);
  if(p == nullptr) return End();
  ValueType value;
  auto leaf = reinterpret_cast<LeafPage *>(p->GetData());
  if(!leaf->Lookup(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
    return End();
  }
  int index = leaf->KeyIndex(key, comparator_);
  return INDEXITERATOR_TYPE(this, leaf, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE(this, nullptr, 0);
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  if(IsEmpty()) return nullptr;
  page_id_t now = root_page_id_;
  Page *p = buffer_pool_manager_->FetchPage(now);
  while(!reinterpret_cast<BPlusTreePage *>(p->GetData())->IsLeafPage()) {
    page_id_t nxt = reinterpret_cast<InternalPage *>(p->GetData())->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(now, false);
    now = nxt;
    p = buffer_pool_manager_->FetchPage(now);
  }
  return p;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int flag) {
  Page *p = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *r = reinterpret_cast<IndexRootsPage *>(p->GetData());
  if(flag == 1) r->Insert(index_id_, root_page_id_);
  else if(flag == 0) r->Update(index_id_, root_page_id_);
  else r->Delete(index_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
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
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
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
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }

      }
    }
  }

}

/**
 * This function is for debug only, you don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);

    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;

template
class BPlusTree<GenericKey<128>, RowId, GenericComparator<128>>;

template
class BPlusTree<GenericKey<256>, RowId, GenericComparator<256>>;
