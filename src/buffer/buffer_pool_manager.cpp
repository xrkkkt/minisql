#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

// ctors have already been given
// BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
//         : pool_size_(pool_size), disk_manager_(disk_manager) {
//     pages_ = new Page[pool_size_];
//     replacer_ = new LRUReplacer(pool_size_);
//     for (size_t i = 0; i < pool_size_; i++) {
//         // initial state: all the pages in the buffer frame is free.
//         // push them all into the free list.
//         free_list_.emplace_back(i);
//     }
// }

// BufferPoolManager::~BufferPoolManager() {
//     for (auto page : page_table_) {
//         // flush all the memory pages into the physical storage(disk)
//         FlushPage(page.first);  // page is a tuple <page_id_t, frame_id_t> -> page.first get the page_id_t of the mapping.
//     }
//     delete[] pages_;
//     delete replacer_;  // call the dtor function of the object replacer_ pointing to.
// }

// Page *BufferPoolManager::FetchPage(page_id_t page_id) {

//     std::scoped_lock lock{latch_};
//     auto search_page = page_table_.find(page_id);
//     if (search_page != page_table_.end()) {
//         // this page exists in the page_table_ (this page is in the buffer pool)
//         frame_id_t frame_id = search_page->second;
//         Page *page = &(pages_[frame_id]);
//         replacer_->Pin(frame_id);
//         page->pin_count_++;
//         return page;
//     } else {
//         frame_id_t frame_id = -1;
//         if (!find_victim_page(&frame_id)) {  // no replacement solution, fetching fails.
//             return nullptr;
//         }
//         // the victim page has been found, now replace the data with the page's content.
//         Page *page = &(pages_[frame_id]);
//         update_page(page, page_id, frame_id);
//         disk_manager_->ReadPage(page_id, page->data_);
//         replacer_->Pin(frame_id);
//         page->pin_count_ = 1;
//         return page;
//     }
// }

// Page *BufferPoolManager::NewPage(page_id_t &page_id) {

//     std::scoped_lock lock{latch_};
//     frame_id_t frame_id = -1;
//     // case 1: can not get victim frame_id, the new page operation fails.
//     if (!find_victim_page(&frame_id)) {
//         return nullptr;
//     }
//     // case 2: got victim frame_id
//     page_id = AllocatePage();
//     Page *page = &(pages_[frame_id]);
//     page->pin_count_ = 1;
//     update_page(page, page_id,
//                 frame_id);
//     replacer_->Pin(frame_id);


//     return page;
// }


// bool BufferPoolManager::DeletePage(page_id_t page_id) {

//     std::scoped_lock lock{latch_};
//     auto search = page_table_.find(page_id);
//     // case 1: the page does not exist, just return true.
//     if (search == page_table_.end()) {
//         return true;
//     }
//     // case 2: normal case, the page exists in the buffer pool
//     frame_id_t frame_id = search->second;
//     Page *page = &(pages_[frame_id]);
//     // case 2.1: the page is still used by some thread, can not delete
//     if (page->pin_count_ > 0) {
//         return false;
//     }

//     DeallocatePage(page_id);
//     update_page(page, INVALID_PAGE_ID, frame_id);
//     free_list_.push_back(frame_id);

//     return true;
// }

// // the page_id argument is the disk page id.
// bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
//     std::scoped_lock lock{latch_};
//     auto search = page_table_.find(page_id);
//     // case 1: the page doesn't exist in the buffer
//     if (search == page_table_.end()) {
//         return false;
//     }

//     // case 2: the page exist in the page table
//     frame_id_t frame_id = search->second;
//     Page *page = &(pages_[frame_id]);
//     // case 2.1: if the pin_count_ = 0; -> the page has not been pinned before
//     if (page->pin_count_ == 0) {
//         return false;
//     }

//     // case 2.2: the pin_count_ > 0
//     page->pin_count_--;
//     if (page->pin_count_ == 0) {

//         replacer_->Unpin(frame_id);
//     }
//     if (is_dirty) {
//         page->is_dirty_ = true;
//     }

//     return true;
// }

// void BufferPoolManager::update_page(Page *page, page_id_t new_page_id, frame_id_t new_frame_id) {
//     // step 1: if it is dirty -> write it back to disk, and set dirty to false.
//     if (page->IsDirty()) {
//         disk_manager_->WritePage(page->page_id_, page->data_);
//         page->is_dirty_ = false;
//     }

//     // step 2: refresh the page table
//     page_table_.erase(page->page_id_);     // delete the page_id and its frame_id in the original page_table_
//     if (new_page_id != INVALID_PAGE_ID) {  // the object contains a physical page. If INVALID_PAGE_ID, then do not add it
//         // to the page_table_
//         page_table_.emplace(new_page_id, new_frame_id);  // add new page_id and the corresponding frame_id into page_table_
//     }

//     // step 3: reset the data in the page(clear out it to be zero), and page id
//     page->ResetMemory();
//     page->page_id_ = new_page_id;
// }

// bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {

//     if (!free_list_.empty()) {
//         *frame_id = free_list_.front();
//         free_list_.pop_front();
//         return true;
//     }

//     return replacer_->Victim(frame_id);
// }


// bool BufferPoolManager::FlushPage(page_id_t page_id) {
//     std::scoped_lock lock{latch_};
//     if (page_id == INVALID_PAGE_ID) {
//         return false;
//     }
//     auto search = page_table_.find(page_id);
//     if (search != page_table_.end()) {
//         frame_id_t frame_id = search->second;
//         Page *page = &(pages_[frame_id]);
//         disk_manager_->WritePage(page->page_id_, page->data_);
//         page->is_dirty_ = false;
//     } else {

//         return false;
//     }

//     return true;
// }

// void BufferPoolManager::FlushAllPages() {
//     std::scoped_lock lock{latch_};
//     for (size_t i = 0; i < pool_size_; i++) {
//         Page *page = &(pages_[i]);
//         if (page->page_id_ != INVALID_PAGE_ID && page->IsDirty()) {
//             disk_manager_->WritePage(page->page_id_, page->data_);
//             page->is_dirty_ = false;
//         }

//     }
// }

// // already implement this function in the disk_manager module.
// // @return value -> the disk page id of next page
// page_id_t BufferPoolManager::AllocatePage() {
//     int next_page_id = disk_manager_->AllocatePage();
//     return next_page_id;
// }

// void BufferPoolManager::DeallocatePage(page_id_t page_id) { disk_manager_->DeAllocatePage(page_id); }

// bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// // Only used for debug
// bool BufferPoolManager::CheckAllUnpinned() {
//     bool res = true;
//     //for (size_t i = 0; i < 100; i++) {
//     //
//     //
//     //    //LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
//     //    cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
//     //
//     //}
//     for (size_t i = 0; i < pool_size_; i++) {
//         if (pages_[i].pin_count_ != 0) {
//             res = false;
//             LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
//             cout << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
//         }
//     }
//     return res;
// }

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);//新建page列表，全在free_list_中
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

// 1.     Search the page table for the requested page (P).
// 1.1    If P exists, pin it and return it immediately.
// 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
//        Note that pages are always found from the free list first.
// 2.     If R is dirty, write it back to the disk.
// 3.     Delete R from the page table and insert P.
// 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  frame_id_t tmp;
  //梁嘉琦加的
  if(page_id > MAX_VALID_PAGE_ID || page_id <= INVALID_PAGE_ID) return nullptr;

  //If P exists, pin it and return it immediately.
  if(page_table_.count(page_id)>0){
    tmp = page_table_[page_id];
    replacer_->Pin(tmp);
    pages_[tmp].pin_count_++;
    return &pages_[tmp];
  }
  //p dos not exist
  if(free_list_.size()>0){//from freelist
    tmp = free_list_.front();//pick from the head of the free list
    page_table_[page_id] = tmp;//update page_table_
    free_list_.pop_front();////delete the frame id from free list

    disk_manager_->ReadPage(page_id, pages_[tmp].data_);
    pages_[tmp].pin_count_ = 1;
    pages_[tmp].page_id_ = page_id;
    
    return &pages_[tmp];
  }
  else{//from replacer
    bool flag = replacer_->Victim(&tmp);
    if(flag==false) return nullptr;
    if(pages_[tmp].IsDirty()){//write back to the disk
      disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());
    }
    //update the pages_
    pages_[tmp].page_id_ = page_id;
    pages_[tmp].pin_count_ = 1;
    page_table_[page_id] = tmp;
    //readpage from disk
    disk_manager_->ReadPage(page_id,pages_[tmp].data_);
    return &pages_[tmp];
  }
  
  return nullptr;
}

// 0.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 1.   If all the pages in the buffer pool are pinned, return nullptr.
// 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
// 3.   Update P's metadata, zero out memory and add P to the page table.
// 4.   Set the page ID output parameter. Return a pointer to P.
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  page_id = 0;
  frame_id_t tmp;
  if(free_list_.size()>0){//freelist first
    page_id = AllocatePage();
    tmp = free_list_.front();//pick from the head of the free list
    free_list_.pop_front();//delete the frame id from free list
  }
  else{//replacer
    bool flag = replacer_->Victim(&tmp);
    if(flag==false) return nullptr;//如果replacer里也没有
    page_id = AllocatePage();//new page_id
    if(pages_[tmp].IsDirty()){
      disk_manager_->WritePage(pages_[tmp].GetPageId(),pages_[tmp].GetData());
    }
    page_table_.erase(pages_[tmp].page_id_);//由于替换了page_id，要删除相应old值
  }
  //Update P's metadata
  pages_[tmp].ResetMemory();
  pages_[tmp].page_id_ = page_id;
  pages_[tmp].pin_count_ = 1;
  page_table_[page_id] = tmp;
  return &pages_[tmp];
}
// 0.   Make sure you call DeallocatePage!
// 1.   Search the page table for the requested page (P).
// 1.   If P does not exist, return true.
// 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
// 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  if(page_table_.count(page_id)==0)
    return true;
  //Search the page table for the requested page
  frame_id_t tmp = page_table_[page_id];
  //If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(pages_[tmp].pin_count_>0)
    return false;
  //delete
  page_table_.erase(page_id);
  pages_[tmp].ResetMemory();
  pages_[tmp].page_id_=INVALID_PAGE_ID;
  pages_[tmp].is_dirty_=false;
  free_list_.push_back(tmp);
  disk_manager_->DeAllocatePage(page_id);//call DeallocatePage
  return true;
 
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_table_.count(page_id)==0) 
    return false;
  frame_id_t tmp = page_table_[page_id];
  if(pages_[tmp].pin_count_==0) 
    return true;
  pages_[tmp].pin_count_--;
  replacer_->Unpin(tmp);
  if(is_dirty) pages_[tmp].is_dirty_ = true;
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();
  frame_id_t tmp = page_table_[page_id];
  if(page_table_.count(page_id)>0){
    disk_manager_->WritePage(page_id, pages_[tmp].data_);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}