#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */




TableIterator::~TableIterator() {
    // 释放动态内存等操作
    delete table_heap_;
    // ...
}

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : table_heap_(table_heap), rid_(rid), txn_(txn) {
  if (!table_heap_->GetTuple(&row_, txn_)) {
      rid_ = INVALID_ROWID;
      row_ = Row();
  }
}





bool TableIterator::operator==(const TableIterator &itr) const {
  if (itr.table_heap_ != table_heap_) {
      return false;
  }
  return rid_ == itr.rid_;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return row_;
}

Row *TableIterator::operator->() {
  return &row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
      table_heap_ = itr.table_heap_;
      rid_ = itr.rid_;
      txn_ = itr.txn_;
      row_ = itr.row_;
  }
  return *this;
}

TableIterator &TableIterator::operator++() {
    // check tableHeap and txn validation
    if(table_heap_==nullptr || txn_==nullptr){
    return *this;
    }
    
    // Create id for the next row
    RowId nextRowId;
    bool found = false;

    // fetch page of the current row
    auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));

    if(page != nullptr) {
        // try to get next row in the current page
        found = page->GetNextTupleRid(rid_, &nextRowId);
        table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);
    }

    // If not found in the current page, try to find the next row in the following pages
    page_id_t nextPageId = page->GetNextPageId();
    while (!found && nextPageId != INVALID_PAGE_ID) {
        page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(nextPageId));

        if(page != nullptr) {
        RowId firstRowIdInNextPage;
        if(page->GetFirstTupleRid(&firstRowIdInNextPage)) {
            nextRowId = firstRowIdInNextPage;
            found = true;
        }

        table_heap_->buffer_pool_manager_->UnpinPage(nextPageId, false);
        }
        nextPageId = page->GetNextPageId();
    }

    // If the next row is found, update the iterator's row id. Otherwise, update the iterator to the end
    if (found) {
        rid_ = nextRowId;
    }
    else {
        *this = table_heap_->End();
    }

    return *this;
}




TableIterator TableIterator::operator++(int) {
  TableIterator tmp(*this);
  operator++();
  return tmp;
}