#include "catalog/table.h"
#include <_types/_uint32_t.h>
#include "common/config.h"
#include "common/macros.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf,TABLE_METADATA_MAGIC_NUM);
  ofs+=sizeof(uint32_t);
  MACH_WRITE_TO(table_id_t,buf+ofs,table_id_);
  ofs+=sizeof(table_id_t);
  uint32_t len=table_name_.length();
  MACH_WRITE_UINT32(buf+ofs, len);
  MACH_WRITE_STRING(buf+ofs+sizeof(uint32_t),table_name_);
  ofs+=MACH_STR_SERIALIZED_SIZE(table_name_);
  MACH_WRITE_TO(page_id_t,buf+ofs,root_page_id_);
  ofs+=sizeof(page_id_t);
  ofs+=schema_->SerializeTo(buf+ofs);
  return ofs;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(uint32_t)+sizeof(table_id_t)+MACH_STR_SERIALIZED_SIZE(table_name_)+sizeof(page_id_t)+schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t ofs=0;
  uint32_t Magic=MACH_READ_UINT32(buf);
  ASSERT(Magic==TABLE_METADATA_MAGIC_NUM,"Deserializing of TableMetadata Failed!");
  ofs+=sizeof(uint32_t);
  table_id_t table_id=MACH_READ_FROM(table_id_t, buf+ofs);
  ofs+=sizeof(table_id_t);
  uint32_t l = MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  std::string name="";
  for(uint32_t i=0;i<l;i++){
    name+=buf[ofs];
    ofs++;
  }
  page_id_t root_page_id=MACH_READ_FROM(page_id_t, buf+ofs);
  ofs+=sizeof(page_id_t);
  TableSchema *schema=nullptr;
  Schema::DeserializeFrom(buf+ofs, schema, heap);
  table_meta=TableMetadata::Create(table_id, name, root_page_id, schema,heap);
  return ofs;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name,
                                     page_id_t root_page_id, TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new(buf)TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
        : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
