// #include "storage/table_heap.h"

// /**
//  * TODO: Student Implement
//  */
// bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
  
// page_id_t curr_page_id = first_page_id_;
//   while(curr_page_id != INVALID_PAGE_ID) {
//     auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(curr_page_id));
//     assert(page != nullptr);
//     if(page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
//       buffer_pool_manager_->UnpinPage(curr_page_id, true);
//       return true;
//     }
//     buffer_pool_manager_->UnpinPage(curr_page_id, false);
//     curr_page_id = page->GetNextPageId();
//   }

//   // 如果没有找到能容纳该记录的TablePage，创建一个新的
//   page_id_t new_page_id;
// auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
//   if (page == nullptr) {
//     return false;
//   }
//   page->Init(curr_page_id, INVALID_PAGE_ID, log_manager_, txn);
//   bool result = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
//   buffer_pool_manager_->UnpinPage(curr_page_id, true);
//   return result;
//   // uint32_t tuple_size = row.GetSerializedSize(schema_);
//   // if(tuple_size>PAGE_SIZE){
//   //   return false;
//   // }
//   // auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
//   // if(page== nullptr)return false;
//   // page->WLatch();
//   // std::cout<<"schema_:"<<schema_->GetColumnCount()<<endl;
//   // bool insertResult = page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
//   // page->WUnlatch();
//   // buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
//   // while(!insertResult){//break if success
//   //   page_id_t next_id = page->GetNextPageId();
//   //   if(next_id!=INVALID_PAGE_ID){
//   //     page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_id));
//   //     page->WLatch();
//   //     insertResult = page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
//   //     page->WUnlatch();
//   //     buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
//   //   }else{
//   //     //attach a page
//   //     page_id_t new_page_id;
//   //     buffer_pool_manager_->NewPage(new_page_id);
//   //     auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(new_page_id));
//   //     new_page->WLatch();
//   //     new_page->Init(new_page_id,page->GetPageId(),log_manager_,txn);
//   //     new_page->SetNextPageId(INVALID_PAGE_ID);
//   //     page->WLatch();
//   //     page->SetNextPageId(new_page_id);
//   //     page->WUnlatch();
//   //     buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
//   //     insertResult = new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
//   //     new_page->WUnlatch();
//   //     buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
//   //     break ;
//   //   }
//   // }
//   // if(insertResult)return true;
//   // else return false;
// }  
  
 

// bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
//   // Find the page which contains the tuple.
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   // If the page could not be found, then abort the recovery.
//   if (page == nullptr) {
//     return false;
//   }
//   // Otherwise, mark the tuple as deleted.
//   page->WLatch();
//   page->MarkDelete(rid, txn, lock_manager_, log_manager_);
//   page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
//   return true;
// }

// /**
//  * TODO: Student Implement
//  */
// bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
  
//   std::cout<<"update"<<endl;
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   if (page == nullptr) {
//     return false;
//   }
//   Row old_row;
//   bool result = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
//   buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
//   return result;
//    }

// /**
//  * TODO: Student Implement
//  */
// void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
//   //
//   std::cout<<"apply delete"<<endl;
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   assert(page != nullptr);
//   page->WLatch();
//   page->ApplyDelete(rid, txn, log_manager_);
//   page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
//   // Step1: Find the page which contains the tuple.
//   // Step2: Delete the tuple from the page.
// }

// void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
//   // Find the page which contains the tuple.
//   std::cout<<"rolback delete delete"<<endl;
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
//   assert(page != nullptr);
//   // Rollback to delete.
//   page->WLatch();
//   page->RollbackDelete(rid, txn, log_manager_);
//   page->WUnlatch();
//   buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
// }

// /**
//  * TODO: Student Implement
//  */
// bool TableHeap::GetTuple(Row *row, Txn *txn) { 
//   std::cout<<"get tuple"<<endl;
//   auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
//   if (page == nullptr) {
//     return false;
//   }
//   bool result = page->GetTuple(row, schema_, txn, lock_manager_);
//   buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), true);
//   return result;
// }
 

// void TableHeap::DeleteTable(page_id_t page_id) {
//   if (page_id != INVALID_PAGE_ID) {
//     auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
//     if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
//       DeleteTable(temp_table_page->GetNextPageId());
//     buffer_pool_manager_->UnpinPage(page_id, false);
//     buffer_pool_manager_->DeletePage(page_id);
//   } else {
//     DeleteTable(first_page_id_);
//   }
// }

// /**
//  * TODO: Student Implement
//  */
// TableIterator TableHeap::Begin(Txn *txn) { 
//   std::cout<<"begin"<<endl;
//   if(first_page_id_ == INVALID_PAGE_ID) {
//     return End();  // 如果表为空，返回结束迭代器
//   }
  
//   auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
//   RowId first_rid;
//   // 找到第一页包含的第一个元组的RowId
//   if(first_page->GetFirstTupleRid(&first_rid)) {
//     buffer_pool_manager_->UnpinPage(first_page_id_, false);
//     return TableIterator(this, first_rid, txn);
//   }
//   RowId next_rid;
//    // 如果第一页没有包含任何元组，则找到下一个存在元组的页面
//   if(first_page->GetNextTupleRid(first_rid, &next_rid)) {
//     buffer_pool_manager_->UnpinPage(first_page_id_, false);
//     return TableIterator(this, next_rid, txn);
//   }
//   buffer_pool_manager_->UnpinPage(first_page_id_, false);
//   return End();

// }

// /**
//  * TODO: Student Implement
//  */
// TableIterator TableHeap::End() {  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr); }




#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  auto page_id = first_page_id_;
  TablePage *page = nullptr;

  while (page_id != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (!page) {
      return false;  // Could not fetch the page.
    }

    page->WLatch();
    // Try to insert the tuple. If the insert fails, move onto the next page.
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true); // Mark the page as dirty.
      return true;
    }

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false); // The page is not dirty.

    // Move onto the next page.
    page_id = page->GetNextPageId();
  }

  // If this point has been reached, then there is no space in any existing page and a new page needs to be created.
  page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(page_id));
  if (!page) {
    return false; // Failed to create a new page.
  }

  // Initialize the new page and attempt to insert the tuple again.
  page->WLatch();
  page->Init(page_id, INVALID_PAGE_ID, log_manager_, txn);
  bool success = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();

  // Fetch the first page to update next_page_id.
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  first_page->WLatch();
  first_page->SetNextPageId(page_id);
  first_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, true); // Mark the first page as dirty.

  buffer_pool_manager_->UnpinPage(page_id, true); // Mark the new page as dirty.

  return success;
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
  // Fetch the page on which the tuple to be updated is located.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (!page) {
    return false; // Could not find the page.
  }

  // Fetch the old tuple for update.
  Row old_row;
  if (!page->GetTuple(&old_row, schema_, txn, lock_manager_)) {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false); // Unpin since we're not modifying the page.
    return false; // Could not find the tuple.
  }

  page->WLatch();
  // Attempt to update the tuple.
  bool success = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();

  buffer_pool_manager_->UnpinPage(rid.GetPageId(), success); // If updated, mark the page as dirty.

  return success;
}
/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Fetch the page where the tuple to be deleted is located.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (!page) {
    throw std::runtime_error("Failed to fetch the page containing the tuple to be deleted.");
  }

  // At this point, we have successfully fetched the related page.
  // Now, let's proceed to delete the tuple.

  page->WLatch(); // Acquire write lock since deletion modifies the page

  // Step2: Call ApplyDelete on the fetched page to actually delete the tuple.
  page->ApplyDelete(rid, txn, log_manager_);

  // if (!deletionSuccess) {
  //   // Failed to delete, might due to the tuple is not found or other reasons.
  //   // You might need more complex error handling here.
  //   page->WUnlatch();
  //   buffer_pool_manager_->UnpinPage(rid.GetPageId(), false); // Unpin the page without marking as dirty since deletion failed.
  //   throw std::runtime_error("Failed to apply delete on the tuple.");
  // }

  // Successfully deleted the tuple, now unpin and mark the page as dirty.
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true); // Unpin the page and mark as dirty since deletion succeeded.
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
  // Fetch the page where the tuple is located.
  auto page_id = row->GetRowId().GetPageId();
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  if (!page) {
    return false; // Could not find the page.
  }

  // Attempt to retrieve the tuple.
  page->RLatch();
  bool success = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();

  buffer_pool_manager_->UnpinPage(page_id, false);

  return success;
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
    return End();
    // 如果表为空，返回结束迭代器
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
TableIterator TableHeap::End() {
  return TableIterator(this, RowId(INVALID_PAGE_ID, -1), nullptr);
}
