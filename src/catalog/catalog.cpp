#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
   int cnt=0;
   cnt = 4 + sizeof(std::size_t) * 2;
   cnt += table_meta_pages_.size() * 8;
   cnt += index_meta_pages_.size() * 8;

  return cnt;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
//
  table_names_.clear();
  tables_.clear();
  index_names_.clear();
  indexes_.clear();
  Page* page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  char* buf = page_->GetData();
  if(init){
    catalog_meta_=catalog_meta_->NewInstance();
    catalog_meta_->table_meta_pages_.clear();
    catalog_meta_->index_meta_pages_.clear();
    catalog_meta_->SerializeTo(buf);
  }else{
    catalog_meta_=catalog_meta_->DeserializeFrom(buf);
    for (auto iter : catalog_meta_->table_meta_pages_) {
      LoadTable(iter.first,iter.second);
    }
    for (auto iter : catalog_meta_->index_meta_pages_) {
      LoadIndex(iter.first,iter.second);
    }
  }
  buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, true);
}

// void CatalogManager::LoadTableInfos() {
//     for (auto& [table_id, page_id] : catalog_meta_->table_meta_pages_) {
//         LoadTableInfo(table_id, page_id);
//     }
// }

// void CatalogManager::LoadTableInfo(table_id_t table_id, page_id_t page_id) {
//     Page *iter_page = buffer_pool_manager_->FetchPage(page_id);
//     auto *table_info = TableInfo::DeserializeFrom(iter_page->GetData(), this->heap_);
//     buffer_pool_manager_->UnpinPage(page_id, false);
//     if (table_info) {
//         this->tables_.emplace(table_id, table_info);
//         this->table_names_.emplace(table_info->GetTableName(), table_id);
//     } else {
//         ASSERT(false, "Failed to load table info.");
//     }
// }

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */

dberr_t CatalogManager:: CreateTableMetadata(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info,page_id_t &meta_page_id ,table_id_t &table_id) {
  //  page_id_t meta_page_id = 0;
    Page* meta_page = nullptr;
    page_id_t table_page_id = 0;
    Page* table_page = nullptr;
  //  table_id_t table_id = 0;
    TableMetadata* table_meta_ = nullptr;
    TableHeap* table_heap_ = nullptr;
    TableSchema* schema_ = nullptr;

    table_info = table_info->Create();

    table_id = catalog_meta_->GetNextTableId();

    schema_ = schema_->DeepCopySchema(schema);

    meta_page = buffer_pool_manager_->NewPage(meta_page_id);

    table_page = buffer_pool_manager_->NewPage(table_page_id);

    table_meta_ = table_meta_->Create(table_id,table_name,table_page_id,schema_);
    table_meta_->SerializeTo(meta_page->GetData());

    table_heap_ = table_heap_->Create(buffer_pool_manager_,schema_, nullptr,nullptr,nullptr);

    table_info->Init(table_meta_,table_heap_);
    
    return DB_SUCCESS;
}



dberr_t CatalogManager:: AddTableToCatalog(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info,page_id_t &meta_page_id ,table_id_t &table_id) {
   
    table_names_[table_name] = table_id;
    tables_[table_id] = table_info;

    
    catalog_meta_->table_meta_pages_[table_id] = meta_page_id;
    Page* page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char* buf = page_->GetData();
    catalog_meta_->SerializeTo(buf);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
    return DB_SUCCESS;
}







dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
 // 检查表名是否已存在

   auto iter = table_names_.find(table_name);
    if(iter!=table_names_.end()){
      return DB_TABLE_ALREADY_EXIST;
    };
      dberr_t ret;
     page_id_t meta_page_id = 0;
     table_id_t table_id = 0;
    ret = CreateTableMetadata(table_name, schema, txn, table_info, meta_page_id, table_id);
    if (ret != DB_SUCCESS) {
        return ret;
    }
    

ret = AddTableToCatalog(table_name, schema, txn, table_info, meta_page_id, table_id);
    return ret;

    
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
 auto it = table_names_.find(table_name);
    if (it == table_names_.end()) {
        return DB_TABLE_NOT_EXIST; // 表不存在
    }
    table_info = tables_.at(it->second);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  if (this->tables_.empty()) {
        return DB_FAILED; // 没有表
    }
    for (auto &entry : this->tables_) {
        tables.push_back(entry.second);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */



dberr_t  CatalogManager::CreateIndexMetadata(const std::string &table_name, const string &index_name,
                            const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                            const string &index_type, index_id_t &index_id, table_id_t &table_id, page_id_t &meta_page_id ) {
    // init
   // table_id_t table_id = 0;
    TableSchema* schema_ = nullptr;
    TableInfo* table_info_ = nullptr;

  //  page_id_t meta_page_id = 0;
    Page* meta_page = nullptr;
//    index_id_t index_id = 0;
    IndexMetadata* index_meta_ = nullptr;
  
    std::vector<std::uint32_t> key_map{};
 
    index_info = index_info->Create();

    index_id = catalog_meta_->GetNextIndexId();
   
    table_id = table_names_[table_name];
    table_info_ = tables_[table_id];
    schema_ = table_info_->GetSchema();

    uint32_t column_index = 0;
    for(auto column_name:index_keys){
      if(schema_->GetColumnIndex(column_name,column_index) == DB_COLUMN_NAME_NOT_EXIST){
        return DB_COLUMN_NAME_NOT_EXIST;
      }
      key_map.push_back(column_index);
    }
    
    meta_page = buffer_pool_manager_->NewPage(meta_page_id);
   
    index_meta_ = index_meta_->Create(index_id,index_name,table_id,key_map);
    index_meta_->SerializeTo(meta_page->GetData());
  
    index_info->Init(index_meta_,table_info_,buffer_pool_manager_);
    
    return DB_SUCCESS;
}





dberr_t  CatalogManager::AddIndexToCatalog(const std::string &table_name, const string &index_name,
                          const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                          const string &index_type, index_id_t &index_id, table_id_t &table_id, page_id_t &meta_page_id) {
    index_names_[table_name][index_name] = index_id;
    indexes_[index_id] = index_info;

    catalog_meta_->index_meta_pages_[index_id] = meta_page_id;
    Page* page_ = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char* buf = page_->GetData();
    catalog_meta_->SerializeTo(buf);
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
    return DB_SUCCESS;
}






dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  auto iter_find_table = table_names_.find(table_name);
    if(iter_find_table==table_names_.end()){
      return DB_TABLE_NOT_EXIST;
    }
    
 
    auto iter_find_index_table = index_names_.find(table_name);
    if(iter_find_index_table!=index_names_.end()){
      auto iter_find_index_name = iter_find_index_table->second.find(index_name);
      if(iter_find_index_name!=iter_find_index_table->second.end()){
        return DB_INDEX_ALREADY_EXIST;
      }
    }

    dberr_t ret;
    page_id_t meta_page_id = 0;
    index_id_t index_id = 0;
    table_id_t table_id = 0;
    
    ret = CreateIndexMetadata(table_name, index_name, index_keys, txn, index_info, index_type, index_id, table_id, meta_page_id);
    if (ret != DB_SUCCESS) {
        return ret;
    }

    ret = AddIndexToCatalog(table_name, index_name, index_keys, txn, index_info, index_type, index_id, table_id, meta_page_id);
    return ret;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {

  if(index_names_.find(table_name)==index_names_.end())  return DB_TABLE_NOT_EXIST;
         
  auto indname_id=index_names_.find(table_name)->second;
  if(indname_id.find(index_name)==indname_id.end()) return DB_INDEX_NOT_FOUND;

  index_id_t index_id=indname_id[index_name];
  index_info=indexes_.find(index_id)->second;

  return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
 auto table_indexes = index_names_.find(table_name);
  if(table_indexes == index_names_.end())
    return DB_TABLE_NOT_EXIST;
  //update indexes
  auto indexes_map = table_indexes->second;
  for(auto it:indexes_map){
    indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto it = table_names_.find(table_name);
    if (it == table_names_.end()) {
        return DB_TABLE_NOT_EXIST; // 表不存在
    }

    // 删除表页面
    table_id_t table_id = it->second;
    page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
    buffer_pool_manager_->DeletePage(page_id);
    
    // 更新目录元数据和内存映射
    catalog_meta_->table_meta_pages_.erase(table_id);
    tables_.erase(table_id);
    table_names_.erase(table_name);

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
   auto iter_find_index_table = index_names_.find(table_name);
    if(iter_find_index_table!=index_names_.end()){
      auto iter_find_index_name = iter_find_index_table->second.find(index_name);
      if(iter_find_index_name==iter_find_index_table->second.end()){//没有找到
        return DB_INDEX_NOT_FOUND;
      }else{
        //删除索引
        delete indexes_[iter_find_index_name->second];
        indexes_.erase(iter_find_index_name->second);
        //删除索引名
        iter_find_index_table->second.erase(index_name);
        return DB_SUCCESS;
      }
    }else{
      return DB_INDEX_NOT_FOUND;
    }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
        return DB_SUCCESS;
    }
    return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  page_id_t meta_page_id=0;
    Page* meta_page=nullptr;
    page_id_t table_page_id=0;
    Page* table_page=nullptr;
    string table_name_="";
    TableMetadata* table_meta_=nullptr;
    TableHeap* table_heap_=nullptr;
    TableSchema* schema_=nullptr;
    TableInfo* table_info=nullptr;
  
    table_info=table_info->Create();
    
    meta_page_id=page_id;
    meta_page=buffer_pool_manager_->FetchPage(meta_page_id);

    table_meta_->DeserializeFrom(meta_page->GetData(),table_meta_);

    ASSERT(table_id==table_meta_->GetTableId(),"Load wrong table");
    table_name_=table_meta_->GetTableName();
    table_page_id=table_meta_->GetFirstPageId();
    schema_=table_meta_->GetSchema();
    table_heap_=table_heap_->Create(buffer_pool_manager_,table_page_id,schema_,nullptr,nullptr);

    table_info->Init(table_meta_,table_heap_);

    table_names_[table_name_]=table_id;
    tables_[table_id]=table_info;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  string table_name="";
    table_id_t table_id=0;
    TableSchema* schema_=nullptr;
    TableInfo* table_info_=nullptr;

    string index_name="";
    page_id_t meta_page_id=0;
    Page* meta_page=nullptr;
    IndexMetadata* index_meta_=nullptr;
    IndexInfo* index_info=nullptr;
  
    std::vector<std::uint32_t> key_map{};
 
    index_info=index_info->Create();
   
    meta_page_id=page_id;
    meta_page=buffer_pool_manager_->FetchPage(meta_page_id);
   
    index_meta_->DeserializeFrom(meta_page->GetData(),index_meta_);
   
    index_name=index_meta_->GetIndexName();
   
    table_id=index_meta_->GetTableId();
  
    table_info_=tables_[table_id];
 
    table_name=table_info_->GetTableName();

    index_info->Init(index_meta_,table_info_,buffer_pool_manager_);

    index_names_[table_name][index_name]=index_id;
    indexes_[index_id]=index_info;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto it = tables_.find(table_id);
  if(it==tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = it->second;
  return DB_SUCCESS;
}