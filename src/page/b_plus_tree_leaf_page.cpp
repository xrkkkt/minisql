#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    SetKeySize(key_size);
    SetMaxSize(max_size);
    SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
    return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
    next_page_id_ = next_page_id;
    if (next_page_id == 0) {
        LOG(INFO) << "Fatal error";
    }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
    int left = 0, right = GetSize(), mid, cmp_res;
    //if(KM.CompareKeys(key, KeyAt(right)) > 0) return right;
    while(left < right){
        mid = (left + right) / 2;
        GenericKey* mid_key = KeyAt(mid);
        cmp_res = KM.CompareKeys(key, mid_key);
        if(cmp_res == -1) right = mid;
        else if(cmp_res == 1) left = mid + 1;  //保证大于key
        else if(cmp_res == 0) return mid;
    }
    return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
    memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
    return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
    *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
    return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
    memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
    int cur_size = GetSize();
    if(cur_size == 0){ //空
        SetKeyAt(0, key);
        SetValueAt(0, value);
        SetSize(1);
    }else{
        int index = KeyIndex(key, KM);
        PairCopy(PairPtrAt(index+1), PairPtrAt(index), cur_size - index); //index后的全部右移一位
        SetKeyAt(index, key);  //插入到index位置
        SetValueAt(index, value);
        SetSize(cur_size + 1);
    }
    return cur_size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {  //从当前page拷贝一半数据到recipient page
    int size = GetSize();
    recipient->CopyNFrom(PairPtrAt(size -size / 2), size / 2);
    SetSize(size - size / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {  //从src中拷贝size大小的数据到当前page
    int cur_size = GetSize();
    PairCopy(PairPtrAt(cur_size), src, size);
    SetSize(cur_size + size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
    int index = KeyIndex(key, KM), cur_size = GetSize();

    if(index >= cur_size) return false; //超出上限
    if(KM.CompareKeys(key, KeyAt(index)) != 0) return false;  //不存在

    value = ValueAt(index);  //有key对应的value
    return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    int cur_size = GetSize();
    RowId value;
    bool flag = Lookup(key, value, KM);
    if(flag == true){  //要被删除的key存在
        int index = KeyIndex(key, KM);
        PairCopy(PairPtrAt(index), PairPtrAt(index+1), cur_size - 1 - index); //index+1开始全部左移一位，覆盖index
        SetSize(cur_size - 1);
        return cur_size - 1;
    }
    return cur_size;  //不存在，不用执行删除操作
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
    page_id_t next_page_id = GetNextPageId();
    int size = GetSize();
    recipient->CopyNFrom(PairPtrAt(0), size);  //全部拷贝到recipient page
    recipient->SetNextPageId(next_page_id);
    SetSize(0);  //清空当前page
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
    int cur_size = GetSize();
    GenericKey* key = KeyAt(0);
    RowId value = ValueAt(0);
    recipient->CopyLastFrom(key, value);  //将当前page的首位拷贝到recipient page的末尾
    SetSize(cur_size - 1);
    PairCopy(PairPtrAt(0), PairPtrAt(1), cur_size - 1);  //左移覆盖当前page首位
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
    int cur_size = GetSize();
    SetSize(cur_size + 1);
    SetKeyAt(cur_size, key);
    SetValueAt(cur_size, value);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
    int cur_size = GetSize();
    GenericKey* key = KeyAt(cur_size - 1);
    RowId value = ValueAt(cur_size - 1);
    recipient->CopyFirstFrom(key, value);  //将当前page的末位拷贝到recipient page的首位
    SetSize(cur_size - 1);  //移除当前page末尾
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
    int cur_size = GetSize();
    SetSize(cur_size + 1);
    PairCopy(PairPtrAt(1), PairPtrAt(0), cur_size); //整体右移一位
    SetKeyAt(0, key);
    SetValueAt(0, value);
}