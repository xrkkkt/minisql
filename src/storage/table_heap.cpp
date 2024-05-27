#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
  
page_id_t curr_page_id = first_page_id_;
  while(curr_page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(curr_page_id));
    assert(page != nullptr);
    if(page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(curr_page_id, true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(curr_page_id, false);
    curr_page_id = page->GetNextPageId();
  }

  // 如果没有找到能容纳该记录的TablePage，创建一个新的
  page_id_t new_page_id;
auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  if (page == nullptr) {
    return false;
  }
  page->Init(curr_page_id, INVALID_PAGE_ID, log_manager_, txn);
  bool result = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(curr_page_id, true);
  return result;
}  
  
 

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
  
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row old_row;
  bool result = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
  return result;
   }

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {

  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    return false;
  }
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), true);
  return result;
}
 

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  
  if(first_page_id_ == INVALID_PAGE_ID) {
    return End();  // 如果表为空，返回结束迭代器
  }
  
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId first_rid;
  // 找到第一页包含的第一个元组的RowId
  if(first_page->GetFirstTupleRid(&first_rid)) {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return TableIterator(this, first_rid, txn);
  }
  RowId next_rid;
   // 如果第一页没有包含任何元组，则找到下一个存在元组的页面
  if(first_page->GetNextTupleRid(first_rid, &next_rid)) {
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return TableIterator(this, next_rid, txn);
  }
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return End();

}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr); }
