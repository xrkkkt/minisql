// #include "record/row.h"

// /**
//  * TODO: Student Implement
//  */
// uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
//   ASSERT(schema != nullptr, "Invalid schema before serialize.");
//   ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
//  // std::cout<<"schema->GetColumnCount():"<<schema->GetColumnCount()<<std::endl;
//   char *original_buf = buf;
//   if(buf==nullptr) std::cout<<"buf is null"<<std::endl;
//     uint32_t num_fields = fields_.size();
//     MACH_WRITE_UINT32(buf, num_fields);
//     //std::cout<<"num_fields:"<<num_fields<<std::endl;
//     buf += sizeof(uint32_t);

//     // Write null bitmap
//     for(auto field : fields_) {
//         *buf++ = field->IsNull() ? '1' : '0';
//     }

//     for(auto field : fields_) {
//         if (!field->IsNull())
//           buf += field->SerializeTo(buf);
//     }
//     //std::cout<<"row ser finish!"<<std::endl;
//     return buf - original_buf;
// }

// uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
//   ASSERT(schema != nullptr, "Invalid schema before serialize.");
//   ASSERT(fields_.empty(), "Non empty field in row.");

// destroy();
//     char *original_buf = buf;
//     uint32_t num_fields = MACH_READ_UINT32(buf);
//     buf += sizeof(uint32_t);

//     std::vector<bool> is_null(num_fields);
//     for(uint32_t i = 0; i < num_fields; i++) {
//         is_null[i] = (*buf++ == '1');
//     }

//     for(uint32_t i = 0; i < num_fields; i++) {
//         Field *field;
//         if(is_null[i]) {
//             field = new Field(schema->GetColumn(i)->GetType());
//         } else {
//             buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &field, false);
//         }
//         fields_.push_back(field);
//     }

//     return buf - original_buf;


// }

// uint32_t Row::GetSerializedSize(Schema *schema) const {
//   ASSERT(schema != nullptr, "Invalid schema before serialize.");
//   ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
//   uint32_t size = 4; // number of fields
//     size += fields_.size(); // null bitmap

//     for(uint32_t i = 0; i < fields_.size(); i++){
//         if (!fields_[i]->IsNull())
//             size += fields_[i]->GetSerializedSize();
//     }

//     return size;
// }

// void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
//   auto columns = key_schema->GetColumns();
//   std::vector<Field> fields;
//   uint32_t idx;
//   for (auto column : columns) {
//     schema->GetColumnIndex(column->GetName(), idx);
//     fields.emplace_back(*this->GetField(idx));
//   }
//   key_row = Row(fields);
// }
#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  char *original_buf = buf;
  uint32_t num_fields = fields_.size();
  MACH_WRITE_UINT32(buf, num_fields);
  buf += sizeof(uint32_t);

  // Allocate space for null bitmap
  char *bitmap = buf;
  buf += num_fields;

  for(uint32_t i = 0; i < num_fields; i++) {
    if (fields_[i]->IsNull()) {
      bitmap[i] = '1';
    } else {
      bitmap[i] = '0';
      buf += fields_[i]->SerializeTo(buf);
    }
  }

  return buf - original_buf;
}


uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  destroy();
  char *original_buf = buf;
  uint32_t num_fields = MACH_READ_UINT32(buf);
  buf += sizeof(uint32_t);

  std::vector<bool> is_null(num_fields);
  for(uint32_t i = 0; i < num_fields; i++) {
    is_null[i] = (*buf++ == '1');
  }

  for(uint32_t i = 0; i < num_fields; i++) {
    Field *field = new Field(schema->GetColumn(i)->GetType());
    if(!is_null[i]) {
      buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &field, false);
    }
    fields_.push_back(field);
  }

  return buf - original_buf;
}



uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 4; // number of fields
  size += fields_.size(); // null bitmap

  for(uint32_t i = 0; i < fields_.size(); i++){
    if (!fields_[i]->IsNull())
      size += fields_[i]->GetSerializedSize();
  }

  return size;
}



void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
  key_row.SetRowId(this->GetRowId());
}
