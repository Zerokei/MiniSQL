#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  bool flag=false;
  while(!flag){
    if(page==nullptr)break;
    flag=page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),flag);
    if(flag)break;
    if(page->GetNextPageId()==INVALID_PAGE_ID)break;
    page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }
  printf("Cannot insert this tuple!\n");
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));  
  Row old_row=Row(rid);
  int32_t flag=page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_);
  if(flag==0){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
  else if(flag==1){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }
  RowId nowid,nxtid;
  bool f=page->GetFirstTupleRid(&nowid);
  if(f!=0){
    while(f){
      f=page->GetNextTupleRid(nowid,&nxtid);
      page->ApplyDelete(nowid,txn,log_manager_);
      nowid=nxtid;
    }
  }
  flag=page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_);
  if(flag==1)buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  else buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return (flag==1);
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  ASSERT(page!=nullptr,"The tuple doesn't exist!");
  page->ApplyDelete(rid,txn,log_manager_);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  while(page!=nullptr){
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
    auto nxtpage=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    buffer_pool_manager_->DeletePage(page->GetTablePageId());
    page=nxtpage;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  bool flag=false;
  while(!flag){
    if(page==nullptr)break;
    flag=page->GetTuple(row,schema_,txn,lock_manager_);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false);
    if(flag)return true;
    if(page->GetNextPageId()==INVALID_PAGE_ID)break;
    page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }
  printf("Cannot find this tuple!\n");
  return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  bool flag=false;
  RowId *id=nullptr;
  while(!flag){
    if(page==nullptr)return TableIterator(-1);
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false); 
    flag=page->GetFirstTupleRid(id);
    if(flag){
      Row row(*id);
      page->GetTuple(&row,schema_,txn,lock_manager_);
      return TableIterator(&row,this,page->GetTablePageId());
    }
    if(page->GetNextPageId()==INVALID_PAGE_ID)break;
    page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
  }
  return TableIterator(-1);
}

TableIterator TableHeap::End() {
  return  TableIterator(-1);
}
