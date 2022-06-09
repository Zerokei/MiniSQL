#include "common/macros.h"
#include "page/table_page.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator(){

}

TableIterator::TableIterator(const TableIterator &other) {
  row_=new Row(*other.row_);
  heap_=other.heap_;
  now_page_=other.now_page_;
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(now_page_==itr.now_page_ && now_page_==-1)return true;
  if(itr.row_!=row_)return false;
  if(heap_->buffer_pool_manager_!=itr.heap_->buffer_pool_manager_)return false;
  if(now_page_!=itr.now_page_)return false;
  return true;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this==itr);
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator++() {
  auto page=reinterpret_cast<TablePage *>(heap_->buffer_pool_manager_->FetchPage(now_page_));
  //cout<<row_->GetRowId().GetPageId()<<" "<<row_->GetRowId().GetSlotNum()<<"~~~~"<<endl;
  bool flag=page->GetTuple(row_,heap_->schema_,nullptr,heap_->lock_manager_);
  heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false);
  ASSERT(flag,"This tuple doesn't exist in this page!");
  RowId new_RowId;
  RowId *rid=&new_RowId;
  //cout<<row_->GetRowId().GetPageId()<<" "<<row_->GetRowId().GetSlotNum()<<"xxxxx"<<endl;
  flag=page->GetNextTupleRid(row_->GetRowId(),rid);
  if(flag){
    row_->SetRowId(new_RowId);
    page->GetTuple(row_,heap_->schema_,nullptr,heap_->lock_manager_);
    return *this;
  }
  while(flag==false){
    if(page->GetNextPageId()==INVALID_PAGE_ID){
      this->now_page_=-1;
      return *this;
    }
    now_page_=page->GetNextPageId();
    page=reinterpret_cast<TablePage *>(heap_->buffer_pool_manager_->FetchPage(now_page_));
    heap_->buffer_pool_manager_->UnpinPage(page->GetTablePageId(),false);
    flag=page->GetFirstTupleRid(rid);
    if(flag)row_->SetRowId(*rid);
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator tmp(*this);
  ++(*this);
  return TableIterator(tmp);
}
