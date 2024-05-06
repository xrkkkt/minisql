#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):max_size(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
   std::scoped_lock lock{mutx_};
  if (LRU_list.empty()) {
    return false;
  }  
  *frame_id = LRU_list.back(); 
  LRU_hash.erase(*frame_id);    
  LRU_list.pop_back();          
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
 std::scoped_lock lock{mutx_};
  
  if (LRU_hash.count(frame_id) == 0) {
    return;
  }
  auto iter = LRU_hash[frame_id];
  LRU_list.erase(iter);  
  LRU_hash.erase(frame_id);  
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
   std::scoped_lock lock{mutx_};
 
  if (LRU_hash.count(frame_id) != 0) {
    return;
  }
  
  if (LRU_list.size() == max_size) {
    return;
  }
  LRU_list.push_front(frame_id);
  LRU_hash.emplace(frame_id, LRU_list.begin()); 
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
 return LRU_list.size();
}