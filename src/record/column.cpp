#include "record/column.h"
// #include <_types/_uint32_t.h>
#include "record/type_id.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf,COLUMN_MAGIC_NUM);
  ofs+=sizeof(uint32_t);
  uint32_t len=name_.length();
  MACH_WRITE_UINT32(buf+ofs, len);
  MACH_WRITE_STRING(buf+ofs+sizeof(uint32_t),name_);
  ofs+=MACH_STR_SERIALIZED_SIZE(name_);
  MACH_WRITE_TO(TypeId,buf+ofs,type_);
  ofs+=sizeof(TypeId);
  MACH_WRITE_UINT32(buf+ofs,len_);
  ofs+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+ofs,table_ind_);
  ofs+=sizeof(uint32_t);
  MACH_WRITE_TO(bool,buf+ofs,nullable_);
  ofs+=sizeof(bool);
  MACH_WRITE_TO(bool,buf+ofs,unique_);
  ofs+=sizeof(bool);
  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  //return sizeof(Column)+sizeof(uint32_t);
  return sizeof(uint32_t)*3+sizeof(bool)*2+MACH_STR_SERIALIZED_SIZE(name_)+sizeof(TypeId);
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  uint32_t ofs=0;
  uint32_t Magic=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  if(Magic!=COLUMN_MAGIC_NUM) printf("Serializing failed!");
  uint32_t l = MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  std::string name="";
  for(uint32_t i=0;i<l;i++){
    name+=buf[ofs];
    ofs++;
  }
  TypeId Id=MACH_READ_FROM(TypeId,buf+ofs);
  ofs+=sizeof(TypeId);
  uint32_t len=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  uint32_t table_index=MACH_READ_UINT32(buf+ofs);
  ofs+=sizeof(uint32_t);
  bool Nullable=MACH_READ_FROM(bool,buf+ofs);
  ofs+=sizeof(bool);
  bool Unique=MACH_READ_FROM(bool,buf+ofs);
  ofs+=sizeof(bool);
  if(Id==kTypeChar)column=new(heap->Allocate(sizeof(Column)))Column(name,Id,len,table_index,Nullable,Unique);
  else column=new(heap->Allocate(sizeof(Column)))Column(name,Id,table_index,Nullable,Unique);
  return ofs;
}
