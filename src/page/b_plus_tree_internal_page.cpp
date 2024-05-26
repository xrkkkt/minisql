#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetSize(0);
    SetKeySize(key_size);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
    memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
    return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
    *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
    for (int i = 0; i < GetSize(); ++i) {
        if (ValueAt(i) == value)
            return i;
    }
    return -1;
}

void *InternalPage::PairPtrAt(int index) {
    return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
    memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    int size = GetSize();
    if(size == 0) return INVALID_PAGE_ID;
    //if(KM.CompareKeys(key, KeyAt(size-1)) == 1) return ValueAt(size-1); //在最后一个指针指向的子节点处
    int left = 1, right = GetSize() - 1, mid, cmp_res;
    while(left <= right){
        mid = (left + right) / 2;
        GenericKey* mid_key = KeyAt(mid);
        cmp_res = KM.CompareKeys(key, mid_key);
        if(cmp_res == -1) right = mid - 1;
        else if(cmp_res == 1) left = mid + 1;
        else if(cmp_res == 0) return ValueAt(mid);
    }
    return ValueAt(left-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    SetSize(2);
    SetValueAt(0, old_value);
    SetKeyAt(1, new_key);
    SetValueAt(1, new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    int cur_size = GetSize();
    int old_index = ValueIndex(old_value);
    //在old_index后插入新的键值对，先移动，后插入
    PairCopy(PairPtrAt(old_index + 2), PairPtrAt(old_index + 1), cur_size - old_index - 1);
    SetKeyAt(old_index + 1, new_key);
    SetValueAt(old_index + 1, new_value);
    SetSize(cur_size + 1);
    return cur_size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    recipient->CopyNFrom(PairPtrAt(size - size / 2), size / 2, buffer_pool_manager);
    SetSize(size - size / 2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    PairCopy(PairPtrAt(cur_size), src, size);
    SetSize(cur_size + size);
    page_id_t page_id, cur_page_id = GetPageId();
    Page* page;
    BPlusTreePage* bpt_page;  //承接子节点
    for(int i = 0; i < size; i++){  //对所有复制进来的value对应的page改parent page id
        page_id = ValueAt(cur_size + i);
        page = buffer_pool_manager->FetchPage(page_id);  //找到page_id对应page
        bpt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());  //类型转换
        bpt_page->SetParentPageId(cur_page_id);  //重置parent page id
        buffer_pool_manager->UnpinPage(page_id, true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    int cur_size = GetSize();
    PairCopy(PairPtrAt(index), PairPtrAt(index+1), cur_size - 1 - index);
    SetSize(cur_size - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    SetSize(0);
    return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    SetKeyAt(0, middle_key);  //将middle key先插入
    recipient->CopyNFrom(PairPtrAt(0), cur_size, buffer_pool_manager);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    recipient->CopyLastFrom(middle_key, ValueAt(0), buffer_pool_manager);
    Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    page_id_t cur_page_id = GetPageId();
    SetKeyAt(cur_size, key);
    SetValueAt(cur_size, value);
    Page* page = buffer_pool_manager->FetchPage(value);
    auto* bpt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    bpt_page->SetParentPageId(cur_page_id);
    buffer_pool_manager->UnpinPage(value, true);
    SetSize(cur_size + 1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    recipient->SetKeyAt(0, middle_key);  //先插入middle key
    recipient->CopyFirstFrom(ValueAt(cur_size - 1), buffer_pool_manager);  //插入value，移动key
    GenericKey* last_key = KeyAt(cur_size - 1);  //插入key
    recipient->SetKeyAt(0, last_key);
    SetSize(cur_size - 1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int cur_size = GetSize();
    page_id_t cur_page_id = GetPageId();
    PairCopy(PairPtrAt(1), PairPtrAt(0), cur_size);
    SetValueAt(0, value);
    Page* page = buffer_pool_manager->FetchPage(value);
    auto* bpt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    bpt_page->SetParentPageId(cur_page_id);
    buffer_pool_manager->UnpinPage(value, true);
    SetSize(cur_size + 1);
}