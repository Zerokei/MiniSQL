#include "record/row.h"
#include "utils/mem_heap.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t ofs=0;
  MACH_WRITE_TO(RowId,buf,rid_);
  ofs+=sizeof(RowId);
  size_t n=GetFieldCount();
  MACH_WRITE_TO(size_t,buf+ofs,n);
  ofs+=sizeof(size_t);
  unsigned char tmp=0;
  for(uint32_t i=0;i<n;i++){
    Field *f=GetField(i);
    if(f->IsNull())tmp=tmp<<1;
    else tmp=tmp<<1|1;
    if((1+i)%8==0){
      MACH_WRITE_TO(unsigned char,buf+ofs,tmp);
      ofs++;
      tmp=0;
    }
  }
  if(n%8!=0){
    MACH_WRITE_TO(unsigned char,buf+ofs,tmp);
    ofs++;
  }
  for(uint32_t i=0;i<n;i++){
    Field *f=GetField(i);
    if(f->IsNull())continue;
    ofs+=f->SerializeTo(buf+ofs);
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t ofs=0;
  RowId rid=MACH_READ_FROM(RowId,buf);
  rid_=rid;
  ofs+=sizeof(RowId);
  size_t n=MACH_READ_FROM(size_t,buf+ofs);
  ofs+=sizeof(size_t);
  size_t cnt=(n+7)/8;
  uint32_t Ofs=ofs;
  ofs+=cnt;
  unsigned char tmp=MACH_READ_FROM(unsigned char,buf+Ofs);
  size_t m=n>=8?8:n;
  GetFields().clear();
  delete(heap_);
  heap_=new SimpleMemHeap;
  for(size_t i=0;i<n;i++){
    bool flag=(tmp>>(m-1))&1;
    TypeId Id=schema->GetColumn(i)->GetType();
    if(flag){
      if(Id==TypeId::kTypeInt){
        int32_t v=MACH_READ_FROM(int32_t,buf+ofs);
        ofs+=sizeof(int32_t);
        GetFields().push_back(ALLOC_P(heap_,Field)(TypeId::kTypeInt,v));
      }
      else if(Id==TypeId::kTypeFloat){
        float_t v=MACH_READ_FROM(float_t,buf+ofs);
        ofs+=sizeof(float_t);
        GetFields().push_back(ALLOC_P(heap_,Field)(TypeId::kTypeFloat,v));
      }
      else {
        uint32_t len = MACH_READ_UINT32(buf+ofs);
        ofs+=sizeof(uint32_t);
        GetFields().push_back(ALLOC_P(heap_,Field)(TypeId::kTypeChar,buf+ofs,len,true));
        ofs+=len;
      }
    }
    else {
      GetFields().push_back(ALLOC_P(heap_,Field)(Id));
    }
    if((i+1)%8==0){
      Ofs++;
      tmp=MACH_READ_FROM(unsigned char,buf+Ofs);
      m=n-i-1>=8?8:n-i-1;
    }
    else m--;
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  size_t n=schema->GetColumnCount();
  uint32_t ofs=sizeof(RowId)+sizeof(size_t);
  ofs+=(n+7)/8;
  for(uint32_t i=0;i<n;i++){
    Field *fie=GetField(i);
    if(fie->IsNull())continue;
    ofs+=fie->GetSerializedSize();
  }
  return ofs;
}
