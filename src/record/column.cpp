#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t ofs = 0;

    uint32_t len = name_.size();
    memcpy(buf, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    memcpy(buf, &len, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    memcpy(buf, name_.c_str(), len);
    buf += len;
    ofs += len;

    memcpy(buf, &type_, sizeof(TypeId));
    buf += sizeof(TypeId);
    ofs += sizeof(TypeId);

    memcpy(buf, &len_, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    memcpy(buf, &table_ind_, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    memcpy(buf, &nullable_, sizeof(bool));
    buf += sizeof(bool);
    ofs += sizeof(bool);

    memcpy(buf, &unique_, sizeof(bool));
    buf += sizeof(bool);
    ofs += sizeof(bool);

    return ofs;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size = 0;

    // Add the size of the magic number
    size += sizeof(uint32_t);

    // Add the size of the length of the name
    size += sizeof(uint32_t);

    // Add the size of the name
    size += name_.size();

    // Add the size of type_
    size += sizeof(TypeId);

    // Add the size of len_
    size += sizeof(uint32_t);

    // Add the size of table_ind_
    size += sizeof(uint32_t);

    // Add the size of nullable_
    size += sizeof(bool);

    // Add the size of unique_
    size += sizeof(bool);
    
    return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t ofs = 0;
    char* name;
    uint32_t len;
    TypeId type;
    uint32_t len_;
    uint32_t table_ind;
    bool nullable;
    bool unique;

    uint32_t magic_number;
    memcpy(&magic_number, buf, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    if (magic_number != COLUMN_MAGIC_NUM) {
        // Fail here
    }

    memcpy(&len, buf, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    name = new char[len + 1];
    strncpy(name, buf, len);
    name[len] = '\0';
    buf += len;
    ofs += len;

    memcpy(&type, buf, sizeof(TypeId));
    buf += sizeof(TypeId);
    ofs += sizeof(TypeId);

    memcpy(&len_, buf, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    memcpy(&table_ind, buf, sizeof(uint32_t));
    buf += sizeof(uint32_t);
    ofs += sizeof(uint32_t);

    memcpy(&nullable, buf, sizeof(bool));
    buf += sizeof(bool);
    ofs += sizeof(bool);

    memcpy(&unique, buf, sizeof(bool));
    buf += sizeof(bool);
    ofs += sizeof(bool);

    // 用获取的值构建新的Column对象
    column = new Column(name, type, len_, table_ind, nullable, unique);

    delete[] name;
    return ofs;
}
