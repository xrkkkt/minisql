#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          processor_(KM),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
    Page* page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    auto* index_root_page = reinterpret_cast<IndexRootsPage*>(page->GetData());
    index_root_page->GetRootId(index_id, &root_page_id_);
    //UpdateRootPageId(1);  //插入当前根节点到根节点列表
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
    if(leaf_max_size == UNDEFINED_SIZE)
      leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
    if(internal_max_size == UNDEFINED_SIZE)
      internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(page_id_t));
}

void BPlusTree::Destroy(page_id_t current_page_id) {
    buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
    return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
    if(IsEmpty()) return false;  //空
    auto* leaf_page = FindLeafPage(key);
    if(leaf_page == nullptr) return false;
    auto* bpt_leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->GetData());  //key所在叶子节点
    RowId val;
    bool isfound = bpt_leaf_page->Lookup(key, val, processor_);  //在叶子节点中查找key对应的value
    if(isfound == true) result.push_back(val);  //找到则插入到result中
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return isfound;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
    if(IsEmpty()){
        StartNewTree(key, value);
        return true;
    }else{
        return InsertIntoLeaf(key, value, transaction);
    }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
    page_id_t root_page_id;
    Page* page = buffer_pool_manager_->NewPage(root_page_id);
    if(page == nullptr) LOG(ERROR) << "out of memory" << endl;
    auto* root_page = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
    root_page->Init(root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
    root_page->Insert(key, value, processor_);
    root_page_id_ = root_page_id;  //更新根节点page id
    
    UpdateRootPageId(1);  //插入到根节点列表
    
    buffer_pool_manager_->UnpinPage(root_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
    Page* page = FindLeafPage(key);
    RowId val;
    auto* leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
    bool existed = leaf_page->Lookup(key, val, processor_);
    if(!existed){  //不存在，需要插入
        int size_after_insert = leaf_page->Insert(key, value, processor_);  //先插入
        if(size_after_insert == leaf_max_size_){  //已满，需要分裂
            auto* new_leaf_page = Split(leaf_page, transaction);
            InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);  //插入并检查上层
        }
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return !existed;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
    page_id_t new_in_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(new_in_page_id);
    auto* new_in_page = reinterpret_cast<BPlusTreeInternalPage*>(new_page->GetData());  //给新的inner page分配空间
    //初始化
    new_in_page->Init(new_in_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
    node->MoveHalfTo(new_in_page, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(new_in_page_id, true);
    return new_in_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
    page_id_t new_leaf_page_id;
    Page* new_page = buffer_pool_manager_->NewPage(new_leaf_page_id);  //给新的leaf page分配空间
    auto* new_leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(new_page->GetData());
    //初始化
    new_leaf_page->Init(new_leaf_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
    new_leaf_page->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(new_leaf_page_id);  //插入新节点
    node->MoveHalfTo(new_leaf_page);  //将已满节点的一半内容转移到新节点
    buffer_pool_manager_->UnpinPage(new_leaf_page_id, true);
    return new_leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
//传进来的node都是BPlusTreePage类型，无法用KeyAt函数，所以需要传入key参数
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
    page_id_t parent_page_id = old_node->GetParentPageId();
    if(parent_page_id == INVALID_PAGE_ID){  //抵达根节点（可能是internal page也可能是leaf page）
        page_id_t new_root_page_id;
        Page* new_page = buffer_pool_manager_->NewPage(new_root_page_id);
        auto* new_root_page = reinterpret_cast<BPlusTreeInternalPage*>(new_page->GetData());  //新的根节点只能为internal page
        //初始化
        new_root_page->Init(new_root_page_id, INVALID_PAGE_ID, old_node->GetKeySize(), internal_max_size_);
        old_node->SetParentPageId(new_root_page_id);
        new_node->SetParentPageId(new_root_page_id);
        root_page_id_ = new_root_page_id;
        UpdateRootPageId(0);  //更新根节点列表
        buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    }else{
        Page* page = buffer_pool_manager_->FetchPage(parent_page_id);
        auto* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
        int size_after_insert = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        if(size_after_insert == internal_max_size_){  //已满，需要分裂
            auto* new_page = Split(parent_page, transaction);
            InsertIntoParent(parent_page, new_page->KeyAt(0), new_page, transaction);  //插入并检查上层
        }
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
    Page* page = FindLeafPage(key);  //寻找key对应的叶子节点
    if(page == nullptr) return;  //is empty
    BPlusTreeLeafPage* leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
    int pre_size = leaf_page->GetSize(); 
    int cur_size = leaf_page->RemoveAndDeleteRecord(key, processor_);  //删除键值对
    //if(pre_size == cur_size) return;  //不存在相应的键值对
    bool PageDeleted = false;
    if(cur_size < leaf_page->GetMinSize()){  //叶子节点需要被操作
        PageDeleted = CoalesceOrRedistribute<BPlusTreeLeafPage>(leaf_page, transaction);
    }else if(pre_size != cur_size){    //删除后可能要更改祖先节点的键值对
        GenericKey *updateKey = leaf_page->KeyAt(0);
        Page *currPage = buffer_pool_manager_->FetchPage(leaf_page->GetParentPageId());
        auto curr_inpage = reinterpret_cast<::InternalPage *>(currPage);
        page_id_t value = leaf_page->GetPageId();
        if(currPage!= nullptr){   //父节点不为空，即当前删除叶节点不是根节点
            while((!curr_inpage->IsRootPage()) && curr_inpage->ValueIndex(value) == 0){  //对应key[0]一直上溯到根节点
                buffer_pool_manager_->UnpinPage(curr_inpage->GetPageId(), false);  //关闭page（无更改）
                value = curr_inpage->GetPageId();
                currPage = buffer_pool_manager_->FetchPage(curr_inpage->GetParentPageId());
                curr_inpage = reinterpret_cast<::InternalPage *>(currPage);
            }
            if(curr_inpage->ValueIndex(value) != 0 && processor_.CompareKeys(updateKey, curr_inpage->KeyAt(curr_inpage->ValueIndex(value))) != 0){  //找到删除后不匹配的中间节点并更新
                curr_inpage->SetKeyAt(curr_inpage->ValueIndex(value), updateKey);
                buffer_pool_manager_->UnpinPage(curr_inpage->GetPageId(), true);  //关闭page（更改）
            }
        }
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);  //先Unpin才能删除
    if(PageDeleted) buffer_pool_manager_->DeletePage(leaf_page->GetPageId());  //将该叶子节点内容全部转移后 需要删除叶子节点的情况
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
    if(node->IsRootPage()) return AdjustRoot(node);  //根节点，直接调整
    if(node->GetSize() >= node->GetMinSize()) return false;
    page_id_t node_id = node->GetPageId(), parent_id = node->GetParentPageId();
    Page* page = buffer_pool_manager_->FetchPage(parent_id);
    BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());   //找到父节点
    int node_index = parent_page->ValueIndex(node_id);  //找到当前节点的索引
    int neighbor_index = node_index > 0 ? node_index - 1 : node_index + 1;
    page_id_t neighbor_id = parent_page->ValueAt(neighbor_index);
    Page* neighbor_page = buffer_pool_manager_->FetchPage(neighbor_id);
    N* neighbor_node = reinterpret_cast<N*>(neighbor_page->GetData());  //找到邻居节点
    if(node->GetSize() + neighbor_node->GetSize() >= node->GetMaxSize()){
        Redistribute(neighbor_node, node, node_index);
        buffer_pool_manager_->UnpinPage(parent_id, true);
        buffer_pool_manager_->UnpinPage(neighbor_id, true);
        return false;
    }else{
        Coalesce(neighbor_node, node, parent_page, node_index);
        buffer_pool_manager_->UnpinPage(parent_id, true);
        buffer_pool_manager_->UnpinPage(neighbor_id, true);
        return true;
    }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    node->MoveAllTo(neighbor_node);  //Move all the key & value pairs from one page to its sibling page
    parent->Remove(index);  //不再连接父节点
    buffer_pool_manager_->DeletePage(node->GetPageId());  //删除page
    if(parent->GetSize() < parent->GetMinSize()){   //父节点需要被操作
        return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
    }else return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
    parent->Remove(index);
    buffer_pool_manager_->DeletePage(node->GetPageId());
    if(parent->GetSize() < parent->GetMinSize()){
        return CoalesceOrRedistribute<BPlusTree::InternalPage>(parent, transaction);
    }else return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
    page_id_t parent_id = node->GetParentPageId(), neighbor_id = neighbor_node->GetPageId(), node_id = node->GetPageId();
    Page* parent = buffer_pool_manager_->FetchPage(parent_id);
    auto* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(parent->GetData());
    if(index == 0){  //move sibling page's first key & value pair into end of input "node"
        neighbor_node->MoveFirstToEndOf(node);
        int neighbor_index = parent_page->ValueIndex(neighbor_id);  
        parent_page->SetKeyAt(neighbor_index, neighbor_node->KeyAt(0));  //父节点键值对更新
    }else{  //move sibling page's last key & value pair into head of input * "node"
        neighbor_node->MoveLastToFrontOf(node);
        int node_index = parent_page->ValueIndex(node_id);
        parent_page->SetKeyAt(node_index, node->KeyAt(0));  //父节点键值对更新
    }
    buffer_pool_manager_->UnpinPage(parent_id, true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
    page_id_t parent_id = node->GetParentPageId(), neighbor_id = neighbor_node->GetPageId(), node_id = node->GetPageId();
    Page* parent = buffer_pool_manager_->FetchPage(parent_id);
    auto* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(parent->GetData());
    if(index == 0){
        GenericKey *key = neighbor_node->KeyAt(1);
        int neighbor_index = parent_page->ValueIndex(neighbor_id);  
        neighbor_node->MoveFirstToEndOf(node, parent_page->KeyAt(neighbor_index), buffer_pool_manager_);
        parent_page->SetKeyAt(neighbor_index, key);
    }else{
        GenericKey *key = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
        int node_index = parent_page->ValueIndex(node_id);
        neighbor_node->MoveLastToFrontOf(node, parent_page->KeyAt(node_index), buffer_pool_manager_);
        parent_page->SetKeyAt(node_index, key);
    }
    buffer_pool_manager_->UnpinPage(parent_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
    if(old_root_node->IsLeafPage()){  //即是根节点也是叶子节点
        if(old_root_node->GetSize() > 0) return false;
        //auto* root_page = reinterpret_cast<BPlusTreeLeafPage*>(old_root_node);
        root_page_id_ = INVALID_PAGE_ID;  
        UpdateRootPageId(0);   //将根节点列表中对应的root page id更新为无效page id
    }else{  //中间节点类型的根节点
        if(old_root_node->GetSize() > 1) return false;
        auto* root_page = reinterpret_cast<BPlusTreeInternalPage*>(old_root_node);
        page_id_t new_root_page_id = root_page->RemoveAndReturnOnlyChild();  //得到子节点的page id
        Page* new_page = buffer_pool_manager_->FetchPage(new_root_page_id);  //找到子节点page
        auto* new_root_page = reinterpret_cast<BPlusTreePage*>(new_page->GetData());
        new_root_page->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(new_root_page_id, true);
        root_page_id_ = new_root_page_id;
        UpdateRootPageId(0);  //更新根节点列表
    }
    return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
    Page* leftmost_leaf_page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);  //根节点开始一直向左
    if(leftmost_leaf_page == nullptr) return IndexIterator();
    int leftmost_leaf_page_id = leftmost_leaf_page->GetPageId();
    buffer_pool_manager_->UnpinPage(leftmost_leaf_page_id, false);
    return IndexIterator(leftmost_leaf_page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
    Page* leaf_page = FindLeafPage(key, INVALID_PAGE_ID, false);
    if(leaf_page == nullptr) return IndexIterator();
    int leaf_page_id = leaf_page->GetPageId();
    auto* l_page = reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->GetData());
    int index = l_page->KeyIndex(key, processor_);
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return IndexIterator(leaf_page_id, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
    Page* leftmost_leaf_page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);  //根节点开始一直向左
    if(leftmost_leaf_page == nullptr) return IndexIterator();
    int leftmost_leaf_page_id = leftmost_leaf_page->GetPageId();
    auto* leaf_page = reinterpret_cast<BPlusTreeLeafPage*>(leftmost_leaf_page->GetData());
    buffer_pool_manager_->UnpinPage(leftmost_leaf_page_id, false);
    return IndexIterator(leftmost_leaf_page_id, buffer_pool_manager_, leaf_page->GetSize() - 1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
    if(IsEmpty()) return nullptr;

    page_id_t cur_page_id = (page_id == INVALID_PAGE_ID ? root_page_id_ : page_id);  //默认从根节点开始
    Page* page = buffer_pool_manager_->FetchPage(cur_page_id);
    BPlusTreePage* bpt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

    while(bpt_page->IsLeafPage() == false){
        buffer_pool_manager_->UnpinPage(cur_page_id, false);  //unpin，未更改
        BPlusTreeInternalPage* bpt_in_page = reinterpret_cast<BPlusTreeInternalPage*>(bpt_page);  //为inner page
        if(leftMost){
            cur_page_id = bpt_in_page->ValueAt(0);
        }else{
            cur_page_id = bpt_in_page->Lookup(key, processor_);
        }
        page = buffer_pool_manager_->FetchPage(cur_page_id);   //找到下一个page
        bpt_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    }

    return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {  //更新或者插入当前root page到root列表中
    Page* root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
    IndexRootsPage* index_root_page = reinterpret_cast<IndexRootsPage*>(root_page->GetData());
    if(insert_record == 0){
        index_root_page->Update(index_id_, root_page_id_);
    }else{
        index_root_page->Insert(index_id_, root_page_id_);
    }
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
    std::string leaf_prefix("LEAF_");
    std::string internal_prefix("INT_");
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        // Print node name
        out << leaf_prefix << leaf->GetPageId();
        // Print node properties
        out << "[shape=plain color=green ";
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
            << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
            << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
            << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < leaf->GetSize(); i++) {
            Row ans;
            processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
            out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Leaf node link if there is a next page
        if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
            out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
            out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
        }

        // Print parent links if there is a parent
        if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
                << leaf->GetPageId() << ";\n";
        }
    } else {
        auto *inner = reinterpret_cast<InternalPage *>(page);
        // Print node name
        out << internal_prefix << inner->GetPageId();
        // Print node properties
        out << "[shape=plain color=pink ";  // why not?
        // Print data of the node
        out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
        // Print data
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
            << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
        out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
            << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
            << "</TD></TR>\n";
        out << "<TR>";
        for (int i = 0; i < inner->GetSize(); i++) {
            out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
            if (i > 0) {
                Row ans;
                processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
                out << ans.GetField(0)->toString();
            } else {
                out << " ";
            }
            out << "</TD>\n";
        }
        out << "</TR>";
        // Print table end
        out << "</TABLE>>];\n";
        // Print Parent link
        if (inner->GetParentPageId() != INVALID_PAGE_ID) {
            out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
                << inner->GetPageId() << ";\n";
        }
        // Print leaves
        for (int i = 0; i < inner->GetSize(); i++) {
            auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
            ToGraph(child_page, bpm, out, schema);
            if (i > 0) {
                auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
                if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
                    out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
                        << child_page->GetPageId() << "};\n";
                }
                bpm->UnpinPage(sibling_page->GetPageId(), false);
            }
        }
    }
    bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
    if (page->IsLeafPage()) {
        auto *leaf = reinterpret_cast<LeafPage *>(page);
        std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
                  << " next: " << leaf->GetNextPageId() << std::endl;
        for (int i = 0; i < leaf->GetSize(); i++) {
            std::cout << leaf->KeyAt(i) << ",";
        }
        std::cout << std::endl;
        std::cout << std::endl;
    } else {
        auto *internal = reinterpret_cast<InternalPage *>(page);
        std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
        }
        std::cout << std::endl;
        std::cout << std::endl;
        for (int i = 0; i < internal->GetSize(); i++) {
            ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
            bpm->UnpinPage(internal->ValueAt(i), false);
        }
    }
}

bool BPlusTree::Check() {
    bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
    if (!all_unpinned) {
        LOG(ERROR) << "problem in page unpin" << endl;
    }
    return all_unpinned;
}