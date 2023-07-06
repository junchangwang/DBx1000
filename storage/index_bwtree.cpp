//
// Created by chien on 6/2/23.
//

#include <cstdio>

#include "row.h"
#include "index_bwtree.h"
#include "mem_alloc.h"

RC index_bwtree::init(uint64_t part_cnt) {
    this->part_cnt = part_cnt;
    roots = (BwTreeType**) malloc(part_cnt * sizeof(BwTreeType));
    for (uint32_t i = 0; i < part_cnt; ++i) {
        roots[i] = new BwTreeType{};
    }
    return RCOK;
}

RC index_bwtree::init(uint64_t part_cnt, table_t * table) {
    this->table = table;
    init(part_cnt);
    return RCOK;
}

int index_bwtree::index_size() {
    int size = 0;
    int item_cnt = 0;

    if (roots == NULL) {
        // cout << "NULL" << endl;
        return -1;
    }

    for (uint32_t i = 0; i < part_cnt; ++i) {
        auto iter = roots[i]->Begin();
        while (!iter.IsEnd()) {
            item_cnt++;
            size += sizeof(*iter->second);
            iter++;
        }
    }
    return size;
}

index_bwtree::BwTreeType * index_bwtree::find_root(uint64_t part_id) {
    assert(part_id < part_cnt);
    return roots[part_id];
}


bool index_bwtree::index_exist(idx_key_t key) {
    BwTreeType * root = find_root(0);
    assert(root != NULL);

    std::vector<itemid_t *> value_list;
    root->GetValue(key, value_list);

    return value_list.size() > 0;
}

bool index_bwtree::index_exist(idx_key_t key, int part_id) {
    BwTreeType * root = find_root(part_id);
    assert(root != NULL);

    std::vector<itemid_t *> value_list;
    root->GetValue(key, value_list);

    return value_list.size() > 0;
}

RC index_bwtree::index_insert(idx_key_t key, itemid_t * item, int part_id) {
    BwTreeType * root = find_root(part_id);
    assert(root != NULL);

    root->Insert(key, item);
    return RCOK;
}

std::vector<itemid_t *> index_bwtree::bwindex_read(idx_key_t key, int part_id) {
    BwTreeType * root = find_root(part_id);
    std::vector<itemid_t *> item_list;
    root->GetValue(key, item_list);
    return item_list;
}

std::vector<itemid_t *> index_bwtree::bwindex_read(idx_key_t key, int part_id, int thd_id) {
    BwTreeType * root = find_root(part_id);
    std::vector<itemid_t *> item_list;
    root->GetValue(key, item_list);
    return item_list;
}

RC index_bwtree::index_remove(idx_key_t key) {
    RC rc = RCOK;

    BwTreeType * root = find_root(0);
    assert(root != NULL);

    std::vector<itemid_t *> value_list;
    root->GetValue(key, value_list);

    for (auto value : value_list) {
        root->Delete(key, value);
    }
    return rc;
}
