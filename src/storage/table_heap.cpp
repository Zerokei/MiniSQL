#include "storage/table_heap.h"
#include "common/config.h"
#include "page/table_page.h"
#include "storage/table_iterator.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if(row.GetSerializedSize(schema_)>TablePage::SIZE_MAX_ROW){
    printf("Cannot insert this tuple!\n");
    return false;
  }
  if(last_page_id_==0){
    buffer_pool_manager_->NewPage(first_page_id_);
    last_page_id_=first_page_id_;
    auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));
    page->Init(first_page_id_, INVALID_PAGE_ID, log_manager_, txn);
    page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
    buffer_pool_manager_->UnpinPage(last_page_id_,true);   
    return true;
  }
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));
  //buffer_pool_manager_->UnpinPage(last_page_id_,false);
  ASSERT(page->GetNextPageId()==INVALID_PAGE_ID,"Error");
  bool flag=page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
  //buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
  if(flag){
    buffer_pool_manager_->UnpinPage(last_page_id_, true);
    return true;
  }
  page_id_t Nxtid=0;
  buffer_pool_manager_->NewPage(Nxtid);
  auto nxtpage=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(Nxtid));
  nxtpage->Init(Nxtid,page->GetTablePageId(), log_manager_,txn);
  page->SetNextPageId(Nxtid);
  nxtpage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  //buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
  //buffer_pool_manager_->UnpinPage(Nxtid,true);      
  page=nxtpage;
  buffer_pool_manager_->UnpinPage(last_page_id_,true);
  buffer_pool_manager_->UnpinPage(Nxtid,true);
  last_page_id_=Nxtid;
  //cout<<last_page_id_<<endl;
  //printf("%d\n",page->GetTablePageId());
  return true;
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

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  ASSERT(!buffer_pool_manager_->IsPageFree(rid.GetPageId()),"The page of the old rowid doesn't exist!\n");
  if(row.GetSerializedSize(schema_)>TablePage::SIZE_MAX_ROW){
    printf("Cannot update this tuple!\n");
    return false;
  }
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
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return InsertTuple(row, txn);
  /*
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
  return (flag==1);*/
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  ASSERT(!buffer_pool_manager_->IsPageFree(rid.GetPageId()),"The page of the rowid doesn't exist!\n");
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->ApplyDelete(rid,txn,log_manager_);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
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
  if(first_page_id_==0)return;
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  while(true){
    auto nxtpage=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    bool flag=true;
    if(page->GetNextPageId()==INVALID_PAGE_ID)flag=0;
    if(flag)buffer_pool_manager_->UnpinPage(page->GetNextPageId(), true);
    buffer_pool_manager_->DeletePage(page->GetTablePageId());
    if(!flag)return;
    page=nxtpage;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  if(buffer_pool_manager_->IsPageFree(row->GetRowId().GetPageId())){
    printf("Cannot find this tuple!\n");
    return false;
  }
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  bool flag=page->GetTuple(row,schema_,txn,lock_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false);
  if(flag)return true;
  printf("Cannot find this tuple!\n");
  return false;
}

TableIterator TableHeap::Begin(Transaction *txn) {
  if(first_page_id_==0)return TableIterator(-1);
  auto page=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)); 
  //buffer_pool_manager_->UnpinPage(first_page_id_,false); 
  bool flag=false;
  RowId new_id=RowId();
  RowId *id=&new_id;
  while(!flag){
    if(page==nullptr)return TableIterator(-1);
    flag=page->GetFirstTupleRid(id);
    if(flag){
      Row row(new_id);
      page->GetTuple(&row,schema_,txn,lock_manager_);
      TableIterator itr=TableIterator(&row,this,page->GetTablePageId());
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false); 
      return itr;
    }
    if(page->GetNextPageId()==INVALID_PAGE_ID){
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      break;
    }
    auto nxtpage=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false); 
    page=nxtpage;
  }
  return TableIterator(-1);
}

TableIterator TableHeap::End() {
  return  TableIterator(-1);
}
