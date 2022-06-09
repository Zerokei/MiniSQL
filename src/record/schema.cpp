#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf,SCHEMA_MAGIC_NUM);
  ofs+=sizeof(uint32_t);  
  uint32_t len=GetColumnCount();
  MACH_WRITE_UINT32(buf+ofs,len);
  ofs+=sizeof(uint32_t);
  for(uint32_t i=0;i<len;i++){
    uint32_t Ofs=GetColumn(i)->SerializeTo(buf+ofs);
    ofs+=Ofs;
  }
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t ofs=8;
  uint32_t len=GetColumnCount();
  for(uint32_t i=0;i<len;i++){
    uint32_t Ofs=GetColumn(i)->GetSerializedSize();
    ofs+=Ofs;
  }
  return ofs;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  uint32_t ofs=0;
  uint32_t Magic=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  if(Magic!=SCHEMA_MAGIC_NUM) printf("Serializing failed!");
  //schema->GetColumns.clear();
  std::vector<Column *> Vec;
  Vec.clear();
  uint32_t len=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  for(uint32_t i=0;i<len;i++){
    Column *New;
    uint32_t Ofs=Column::DeserializeFrom(buf+ofs,New,heap);
    Vec.push_back(New);
    ofs+=Ofs;
  }
  void *Buf = heap->Allocate(sizeof(Schema));
  schema=new(Buf) Schema(Vec);
  return ofs;
}