#include "catalog/catalog.h"
#include <_types/_uint32_t.h>
#include <cstddef>
#include "buffer/buffer_pool_manager.h"
#include "catalog/indexes.h"
#include "catalog/table.h"
#include "common/config.h"
#include "common/dberr.h"
#include "index/index.h"
#include "storage/table_heap.h"

void CatalogMeta::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf+ofs,CATALOG_METADATA_MAGIC_NUM);
  ofs+=sizeof(uint32_t);
  uint32_t num1=table_meta_pages_.size();
  MACH_WRITE_UINT32(buf+ofs, num1);
  ofs+=sizeof(uint32_t);
  for(auto itr=table_meta_pages_.begin();itr!=table_meta_pages_.end();itr++){
    table_id_t table_id=itr->first;
    MACH_WRITE_TO(table_id_t, buf+ofs, table_id);
    ofs+=sizeof(table_id_t);
    page_id_t page_id=itr->second;
    MACH_WRITE_TO(page_id_t, buf+ofs, page_id);
    ofs+=sizeof(page_id_t);
  }
  uint32_t num2=index_meta_pages_.size();
  MACH_WRITE_UINT32(buf+ofs, num2);
  ofs+=sizeof(uint32_t);
  for(auto itr=index_meta_pages_.begin();itr!=index_meta_pages_.end();itr++){
    table_id_t index_id=itr->first;
    MACH_WRITE_TO(index_id_t, buf+ofs, index_id);
    ofs+=sizeof(index_id_t);
    page_id_t page_id=itr->second;
    MACH_WRITE_TO(page_id_t, buf+ofs, page_id);
    ofs+=sizeof(page_id_t);
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t ofs=0;
  uint32_t Magic=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  ASSERT(Magic==CATALOG_METADATA_MAGIC_NUM,"Serializing of CatalogMeta failed!");
  CatalogMeta *Cata=CatalogMeta::NewInstance(heap);
  Cata->GetTableMetaPages()->clear();
  Cata->GetIndexMetaPages()->clear();
  uint32_t num1=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  for(uint32_t i=0;i<num1;i++){
    table_id_t table_id=MACH_READ_FROM(table_id_t, buf+ofs);
    ofs+=sizeof(table_id_t);
    page_id_t page_id=MACH_READ_FROM(page_id_t, buf+ofs);
    ofs+=sizeof(table_id_t);
    Cata->GetTableMetaPages()->insert(make_pair(table_id,page_id));
  }
  uint32_t num2=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  for(uint32_t i=0;i<num2;i++){
    index_id_t index_id=MACH_READ_FROM(index_id_t, buf+ofs);
    ofs+=sizeof(index_id_t);
    page_id_t page_id=MACH_READ_FROM(page_id_t, buf+ofs);
    ofs+=sizeof(table_id_t);
    Cata->GetIndexMetaPages()->insert(make_pair(index_id,page_id));
  }
  return Cata;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t ofs=0;
  uint32_t num1=table_meta_pages_.size(),num2=index_meta_pages_.size();
  ofs=num1*(sizeof(table_id_t)+sizeof(page_id_t));
  ofs+=num2*(sizeof(index_id_t)+sizeof(page_id_t));
  ofs+=sizeof(uint32_t)*3;
  return ofs;
}

CatalogMeta::CatalogMeta() {

}


CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager),
          log_manager_(log_manager), heap_(new SimpleMemHeap()) {
  catalog_meta_=nullptr;
  if(init){
    catalog_meta_=CatalogMeta::NewInstance(heap_);
    catalog_meta_->GetIndexMetaPages()->clear();
    catalog_meta_->GetTableMetaPages()->clear();
  }
  else {
    Page *page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_=CatalogMeta::DeserializeFrom(page->GetData(),heap_);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,false);
    ASSERT(catalog_meta_!=nullptr,"catalog_meta_ cannot be nullptr!");
  }
  for(auto itr0=catalog_meta_->GetTableMetaPages()->begin();itr0!=catalog_meta_->GetTableMetaPages()->end();itr0++){
    table_id_t table_id=itr0->first;
    page_id_t page_id=itr0->second;
    LoadTable(table_id, page_id);
  }
  for(auto itr1=catalog_meta_->GetIndexMetaPages()->begin();itr1!=catalog_meta_->GetIndexMetaPages()->end();itr1++){
    index_id_t index_id=itr1->first;
    page_id_t page_id=itr1->second;
    LoadIndex(index_id, page_id);
  }
}

CatalogManager::~CatalogManager() {
  delete heap_;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  auto itr=table_names_.find(table_name);
  if(itr!=table_names_.end())return DB_TABLE_ALREADY_EXIST;
  auto *table_in=TableInfo::Create(heap_);
  ASSERT(catalog_meta_!=nullptr,"catalog_meta is null!");
  table_id_t table_id=catalog_meta_->GetNextTableId()+1;
  page_id_t page_id=0;
  Page* page1=buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->GetTableMetaPages()->insert(make_pair(table_id,page_id));
  page_id_t root_page_id=0;
  buffer_pool_manager_->NewPage(root_page_id);
  TableMetadata *TableMeta=TableMetadata::Create(table_id, table_name, root_page_id, schema, heap_);
  TableHeap * table_heap=TableHeap::Create(buffer_pool_manager_,root_page_id,schema,log_manager_,lock_manager_,heap_);
  table_in->Init(TableMeta, table_heap);
  table_info=table_in;
  table_names_.insert((make_pair(table_name,table_id)));
  tables_.insert(make_pair(table_id,table_in));
  TableMeta->SerializeTo(page1->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  Page *page0=buffer_pool_manager_->FetchPage(0);
  catalog_meta_->SerializeTo(page0->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto itr=table_names_.find(table_name);
  if(itr==table_names_.end())return DB_TABLE_NOT_EXIST;
  table_id_t table_id=itr->second;
  //page_id_t page_id=catalog_meta_->GetTableMetaPages()->find(table_id)->second;
  table_info=tables_.find(table_id)->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  tables.clear();
  for(auto itr=table_names_.begin();itr!=table_names_.end();itr++){
    table_id_t table_id=itr->second;
    //page_id_t page_id=catalog_meta_->GetTableMetaPages()->find(table_id)->second;
    TableInfo * table_info=nullptr;
    table_info=tables_.find(table_id)->second;
    tables.push_back(table_info);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  auto itr=table_names_.find(table_name);
  if(itr==table_names_.end())return DB_TABLE_NOT_EXIST;
  auto itr0=index_names_.find(table_name);
  std::unordered_map<std::string, index_id_t>Map;
  Map.clear();
  if(itr0!=index_names_.end())Map=itr0->second;
  auto itr1=Map.find(index_name);
  if(itr1!=Map.end())return DB_INDEX_ALREADY_EXIST;
  table_id_t table_id=itr->second;
  //page_id_t page_id=catalog_meta_->GetTableMetaPages()->find(table_id)->second;
  index_id_t index_id=catalog_meta_->GetNextIndexId()+1;
  page_id_t page_id=0;
  Page * page1=buffer_pool_manager_->NewPage(page_id);
  TableInfo *Table=nullptr;
  GetTable(table_name,Table);
  std::vector<uint32_t> key_map;
  key_map.clear();
  uint32_t len=index_keys.size();
  for(uint32_t i=0;i<len;i++){
    uint32_t index_=0;
    dberr_t Err0=Table->GetSchema()->GetColumnIndex(index_keys[i],index_);
    if(Err0!=DB_SUCCESS)return Err0;
    key_map.push_back(index_);
  }
  IndexMetadata *meta_data=IndexMetadata::Create(index_id,index_name,table_id,key_map,heap_);
  catalog_meta_->GetIndexMetaPages()->insert(make_pair(index_id,page_id));
  index_info=IndexInfo::Create(heap_);
  index_info->Init(meta_data, Table, buffer_pool_manager_);
  if(itr0!=index_names_.end())index_names_.erase(itr0);
  Map.insert(make_pair(index_name,index_id));
  index_names_.insert(make_pair(table_name,Map));
  indexes_.insert(make_pair(index_id,index_info));
  meta_data->SerializeTo(page1->GetData());
  Page *page0=buffer_pool_manager_->FetchPage(0);
  catalog_meta_->SerializeTo(page0->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  //IndexSchema *key_schema=Table->GetSchema()->ShallowCopySchema(Table->GetSchema(), key_map, heap_);
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto itr=table_names_.find(table_name);
  if(itr==table_names_.end())return DB_TABLE_NOT_EXIST;
  auto itr0=index_names_.find(table_name);
  if(itr0==index_names_.end())return DB_INDEX_NOT_FOUND;
  std::unordered_map<std::string, index_id_t>Map=itr0->second;
  auto itr1=Map.find(index_name);
  if(itr1==Map.end())return DB_INDEX_NOT_FOUND;
  index_id_t index_id=itr1->second;
  auto itr2=indexes_.find(index_id);
  if(itr2==indexes_.end())return DB_INDEX_NOT_FOUND;
  index_info=itr2->second;
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  indexes.clear();
  auto itr=table_names_.find(table_name);
  if(itr==table_names_.end())return DB_TABLE_NOT_EXIST;
  auto itr0=index_names_.find(table_name);
  if(itr0==index_names_.end())return DB_INDEX_NOT_FOUND;
  std::unordered_map<std::string, index_id_t>Map=itr0->second;
  for(auto itr1=Map.begin();itr1!=Map.end();itr1++){
    index_id_t index_id=itr1->second;
    auto itr2=indexes_.find(index_id);
    if(itr2==indexes_.end())return DB_INDEX_NOT_FOUND;
    IndexInfo * Index_info=itr2->second;
    indexes.push_back(Index_info);
  }
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  auto itr=table_names_.find(table_name);
  if(itr==table_names_.end())return DB_TABLE_NOT_EXIST;
  table_id_t table_id=itr->second;
  auto itr1=catalog_meta_->GetTableMetaPages()->find(table_id);
  page_id_t page_id=itr1->second;
  auto itr2=tables_.find(page_id);  
  TableInfo *table_info=itr2->second;
  std::vector<IndexInfo *> indexes;
  dberr_t Err0=GetTableIndexes(table_name, indexes);
  if(Err0!=DB_SUCCESS)return Err0;
  uint32_t len=indexes.size();
  for(uint32_t i=0;i<len;i++){
    IndexInfo *index_info=indexes[i];
    dberr_t Err1=DropIndex(table_name, index_info->GetIndexName());
    if(Err1!=DB_SUCCESS)return Err1;
  }
  catalog_meta_->GetTableMetaPages()->erase(itr1);
  tables_.erase(itr2);
  table_names_.erase(itr);
  auto itr3=index_names_.find(table_name);
  if(itr3!=index_names_.end())index_names_.erase(itr3);
  table_info->GetTableHeap()->FreeHeap();
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->DeletePage(page_id);
  delete(table_info);
  Page *page0=buffer_pool_manager_->FetchPage(0);
  catalog_meta_->SerializeTo(page0->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto itr=table_names_.find(table_name);
  if(itr==table_names_.end())return DB_TABLE_NOT_EXIST;
  IndexInfo *index_info=nullptr;
  dberr_t Err0=GetIndex(table_name, index_name, index_info);
  if(Err0!=DB_SUCCESS)return Err0;
  auto itr0=index_names_.find(table_name);
  if(itr0==index_names_.end())return DB_INDEX_NOT_FOUND;
  std::unordered_map<std::string, index_id_t>Map=itr0->second;  
  auto itr1=Map.find(index_name);
  if(itr1==Map.end())return DB_INDEX_NOT_FOUND;
  index_id_t index_id=itr1->second;
  auto itr2=catalog_meta_->GetIndexMetaPages()->find(index_id);
  page_id_t page_id=itr2->second;
  catalog_meta_->GetIndexMetaPages()->erase(itr2);
  auto itr3=indexes_.find(index_id);
  indexes_.erase(itr3);
  index_names_.erase(itr0);
  Map.erase(itr1);
  index_names_.insert(make_pair(table_name,Map));
  buffer_pool_manager_->UnpinPage(page_id,true);
  buffer_pool_manager_->DeletePage(page_id);
  delete(index_info);
  Page *page0=buffer_pool_manager_->FetchPage(0);
  catalog_meta_->SerializeTo(page0->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
  bool flag=buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  if(flag==0)return DB_FAILED;
  else return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(buffer_pool_manager_->IsPageFree(page_id))return DB_FAILED;
  Page *table_meta_page=buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, false);
  TableInfo *table_info=TableInfo::Create(heap_);
  if(table_info==nullptr)return DB_FAILED;
  TableMetadata *table_meta=nullptr;
  auto itr=catalog_meta_->GetTableMetaPages()->find(table_id);
  if(itr==catalog_meta_->GetTableMetaPages()->end())return DB_FAILED;
  TableMetadata::DeserializeFrom(table_meta_page->GetData(),table_meta,table_info->GetMemHeap());
  if(table_meta==nullptr)return DB_FAILED;
  
  TableHeap *table_heap=TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                           log_manager_,lock_manager_,table_info->GetMemHeap());
  table_info->Init(table_meta, table_heap); 
  table_names_.insert(make_pair(table_meta->GetTableName(),table_id));
  tables_.insert(make_pair(table_id,table_info));
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(buffer_pool_manager_->IsPageFree(page_id))return DB_FAILED;
  Page *index_meta_page=buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, false);
  IndexInfo *index_info=IndexInfo::Create(heap_);
  if(index_info==nullptr)return DB_FAILED;
  IndexMetadata *index_meta=nullptr;
  IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta, index_info->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id, false);
  if(index_meta==nullptr)return DB_FAILED;  
  table_id_t table_id=index_meta->GetTableId();
  TableInfo *table_info=TableInfo::Create(index_info->GetMemHeap());
  TableMetadata *table_meta=nullptr;
  auto itr=catalog_meta_->GetTableMetaPages()->find(table_id);
  if(itr==catalog_meta_->GetTableMetaPages()->end())return DB_FAILED;
  page_id_t page_id1=itr->second;
  Page *table_meta_page=buffer_pool_manager_->FetchPage(page_id1);
  TableMetadata::DeserializeFrom(table_meta_page->GetData(),table_meta,table_info->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id1, false);
  if(table_meta==nullptr)return DB_FAILED;
  TableHeap *table_heap=TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                           log_manager_,lock_manager_,table_info->GetMemHeap());
  table_info->Init(table_meta, table_heap);
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.insert(make_pair(index_id,index_info));
  auto itr1=index_names_.find(table_meta->GetTableName());
  std::unordered_map<std::string, index_id_t> Map;
  Map.clear();
  if(itr1!=index_names_.end()){
    Map=itr1->second;
    index_names_.erase(itr1);
  }
  auto itr2=Map.find(index_meta->GetIndexName());
  if(itr2!=Map.end())return DB_FAILED;
  Map.insert(make_pair(index_meta->GetIndexName(),index_id));
  index_names_.insert(make_pair(table_meta->GetTableName(),Map));
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto itr=catalog_meta_->GetTableMetaPages()->find(table_id);
  if(itr==catalog_meta_->GetTableMetaPages()->end())return DB_TABLE_NOT_EXIST;
  page_id_t page_id=itr->second;
  table_info=tables_.find(page_id)->second;
  return DB_SUCCESS;
}