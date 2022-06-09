#include "catalog/indexes.h"
// #include <_types/_uint32_t.h>
#include <sys/types.h>
#include "common/config.h"

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name,
                                     const table_id_t table_id, const vector<uint32_t> &key_map,
                                     MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new(buf)IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf,INDEX_METADATA_MAGIC_NUM);
  ofs+=sizeof(uint32_t);
  MACH_WRITE_TO(index_id_t,buf+ofs,index_id_);
  ofs+=sizeof(index_id_t);
  uint32_t l=index_name_.length();
  MACH_WRITE_UINT32(buf+ofs, l);
  MACH_WRITE_STRING(buf+ofs+sizeof(uint32_t),index_name_);
  ofs+=MACH_STR_SERIALIZED_SIZE(index_name_);
  MACH_WRITE_TO(table_id_t,buf+ofs,table_id_);
  ofs+=sizeof(table_id_t);
  uint32_t len=GetIndexColumnCount();
  MACH_WRITE_UINT32(buf+ofs,len);
  ofs+=sizeof(uint32_t);
  for(uint32_t i=0;i<len;i++){
    uint32_t val_=GetKeyMapping()[i];
    MACH_WRITE_UINT32(buf+ofs, val_);
    ofs+=sizeof(uint32_t);
  }
  return ofs;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  return sizeof(uint32_t)*(GetIndexColumnCount()+2)+sizeof(index_id_t)+sizeof(table_id_t)+MACH_STR_SERIALIZED_SIZE(index_name_);
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  int32_t ofs=0;
  uint32_t Magic=MACH_READ_UINT32(buf);
  if(Magic!=INDEX_METADATA_MAGIC_NUM) printf("Deserializing of TableMetadata Failed!");
  ofs+=sizeof(uint32_t);
  index_id_t index_id=MACH_READ_FROM(index_id_t, buf+ofs);
  ofs+=sizeof(index_id_t);
  uint32_t l = MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  std::string name="";
  for(uint32_t i=0;i<l;i++){
    name+=buf[ofs];
    ofs++;
  }
  table_id_t table_id=MACH_READ_FROM(table_id_t, buf+ofs);
  ofs+=sizeof(table_id_t);
  uint32_t len=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  std::vector<uint32_t> Vec;
  Vec.clear();
  for(uint32_t i=0;i<len;i++){
    uint32_t val_=MACH_READ_UINT32(buf+ofs);
    ofs+=sizeof(uint32_t);
    Vec.push_back(val_);
  }
  index_meta=IndexMetadata::Create(index_id, name, table_id, Vec,heap);
  return ofs;
}