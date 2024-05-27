#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // 保存原始buf指针以便后面计算偏移量
  char *original_buf = buf;
  // 写入魔数
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  buf += sizeof(uint32_t);

  // Write column count
  auto size = GetColumnCount();
  MACH_WRITE_UINT32(buf, size);
  buf += sizeof(uint32_t);

  // 对每一列进行序列化
  for (const auto &column : columns_) {
    buf += column->SerializeTo(buf);
  }

  return buf - original_buf;
}


uint32_t Schema::GetSerializedSize() const {
  uint32_t size = sizeof(SCHEMA_MAGIC_NUM);
  size += sizeof(uint32_t);
  // Add up the serialized size of each column
  for (const auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
 char *original_buf = buf;
  // 检测魔数
  uint32_t magic_number = MACH_READ_UINT32(buf);
  assert(magic_number == SCHEMA_MAGIC_NUM);
  buf += sizeof(uint32_t);

  uint32_t size = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  std::vector<Column*> columns;
  columns.reserve(size);
  
  for(uint32_t i = 0; i < size; i++) {
    Column *column;
    buf += Column::DeserializeFrom(buf, column);
    columns.push_back(column);
  }

  schema = new Schema(columns, true);

  return buf - original_buf;
}