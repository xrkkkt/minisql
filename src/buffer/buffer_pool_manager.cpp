#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

// ctors have already been given
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    // initial state: all the pages in the buffer frame is free.
    // push them all into the free list.
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    // flush all the memory pages into the physical storage(disk)
    FlushPage(page.first);  // page is a tuple <page_id_t, frame_id_t> -> page.first get the page_id_t of the mapping.
  }
  delete[] pages_;
  delete replacer_;  // call the dtor function of the object replacer_ pointing to.
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
 
  std::scoped_lock lock{latch_};
  auto search_page = page_table_.find(page_id);
  if (search_page != page_table_.end()) {
    // this page exists in the page_table_ (this page is in the buffer pool)
    frame_id_t frame_id = search_page->second;
    Page *page = &(pages_[frame_id]);  
    replacer_->Pin(frame_id);         
    page->pin_count_++;                
    return page;                     
  } else {                             
    frame_id_t frame_id = -1;
    if (!find_victim_page(&frame_id)) {  // no replacement solution, fetching fails.
      return nullptr;
    }
    // the victim page has been found, now replace the data with the page's content.
    Page *page = &(pages_[frame_id]);
    update_page(page, page_id, frame_id);
    disk_manager_->ReadPage(page_id, page->data_);  
    replacer_->Pin(frame_id);                      
    page->pin_count_ = 1;  
    return page;
  }
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  
  std::scoped_lock lock{latch_};
  frame_id_t frame_id = -1;
  // case 1: can not get victim frame_id, the new page operation fails.
  if (!find_victim_page(&frame_id)) {
    return nullptr;
  }
  // case 2: got victim frame_id
  page_id = AllocatePage();         
  Page *page = &(pages_[frame_id]);  
  page->pin_count_ = 1;  
  update_page(page, page_id,
              frame_id);     
  replacer_->Pin(frame_id);  
 

  return page;
}


bool BufferPoolManager::DeletePage(page_id_t page_id) {
  
  std::scoped_lock lock{latch_};
  auto search = page_table_.find(page_id);
  // case 1: the page does not exist, just return true.
  if (search == page_table_.end()) {
    return true;
  }
  // case 2: normal case, the page exists in the buffer pool
  frame_id_t frame_id = search->second;
  Page *page = &(pages_[frame_id]);
  // case 2.1: the page is still used by some thread, can not delete
  if (page->pin_count_ > 0) {
    return false;
  }

  DeallocatePage(page_id);                      
  update_page(page, INVALID_PAGE_ID, frame_id);  
  free_list_.push_back(frame_id);                .

  return true;
}

// the page_id argument is the disk page id.
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  auto search = page_table_.find(page_id);
  // case 1: the page doesn't exist in the buffer
  if (search == page_table_.end()) {
    return false;
  }

  // case 2: the page exist in the page table
  frame_id_t frame_id = search->second;
  Page *page = &(pages_[frame_id]);
  // case 2.1: if the pin_count_ = 0; -> the page has not been pinned before
  if (page->pin_count_ == 0) {
    return false;
  }

  // case 2.2: the pin_count_ > 0
  page->pin_count_--;
  if (page->pin_count_ == 0) {
    
    replacer_->Unpin(frame_id);
  }
  if (is_dirty) {
    page->is_dirty_ = true;  
  }

  return true;
}

void BufferPoolManager::update_page(Page *page, page_id_t new_page_id, frame_id_t new_frame_id) {
  // step 1: if it is dirty -> write it back to disk, and set dirty to false.
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }

  // step 2: refresh the page table
  page_table_.erase(page->page_id_);     // delete the page_id and its frame_id in the original page_table_
  if (new_page_id != INVALID_PAGE_ID) {  // the object contains a physical page. If INVALID_PAGE_ID, then do not add it
                                         // to the page_table_
    page_table_.emplace(new_page_id, new_frame_id);  // add new page_id and the corresponding frame_id into page_table_
  }

  // step 3: reset the data in the page(clear out it to be zero), and page id
  page->ResetMemory();
  page->page_id_ = new_page_id;
}

bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {

  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  
  return replacer_->Victim(frame_id);
}


bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::scoped_lock lock{latch_};
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto search = page_table_.find(page_id);  
  if (search != page_table_.end()) {
    frame_id_t frame_id = search->second;
    Page *page = &(pages_[frame_id]);
    disk_manager_->WritePage(page->page_id_, page->data_);  
    page->is_dirty_ = false; .
  } else {
   
    return false;
  }

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock lock{latch_};
  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = &(pages_[i]);
    if (page->page_id_ != INVALID_PAGE_ID && page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
      page->is_dirty_ = false;
    }

  }
}

// already implement this function in the disk_manager module.
// @return value -> the disk page id of next page
page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  //for (size_t i = 0; i < 100; i++) {
  //
  //    
  //    //LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
  //    cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
  //  
  //}
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
      cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}