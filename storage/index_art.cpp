#include <cstdio>

#include "row.h"
#include "index_art.h"
#include "mem_alloc.h"
#include "helper.h"
#include "tpch.h"
#include "tpch_const.h"
#include "chbench_const.h"

void loadKey(TID tid, Key &key) {
    key.setKeyLen(sizeof(tid));
    itemid_t * item = reinterpret_cast<itemid_t *>(tid);
    row_t * row = (row_t *)item->location;

    uint64_t shipdate;
    row->get_value(L_SHIPDATE, shipdate);
    double l_discount;
    row->get_value(L_DISCOUNT, l_discount);
    uint64_t discount = (uint64_t)(l_discount * 100);
    double quantity;
    row->get_value(L_QUANTITY, quantity);
    uint64_t computed_key = tpch_lineitemKey_index(shipdate, discount, (uint64_t)quantity);

    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(computed_key);
}

void loadKey_chbenchq6(TID tid, Key &key) {
    key.setKeyLen(sizeof(tid));
    itemid_t * item = reinterpret_cast<itemid_t *>(tid);
    row_t * row = (row_t *)item->location;
    uint64_t quantity;
    row->get_value(OL_QUANTITY, quantity);
    uint64_t date;
    row->get_value(OL_DELIVERY_D, date);
    uint64_t computed_key = quantity*100000000 + date;
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(computed_key);
}

void loadKey_chbenchq1(TID tid, Key &key) {
    key.setKeyLen(sizeof(tid));
    itemid_t * item = reinterpret_cast<itemid_t *>(tid);
    row_t * row = (row_t *)item->location;
    uint64_t date;
    row->get_value(OL_DELIVERY_D, date);
    uint64_t computed_key =date;
    reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(computed_key);
}

RC index_art::init(uint64_t part_cnt) {
    this->part_cnt = part_cnt;
    roots = (ART_OLC::Tree **) malloc(part_cnt * sizeof(ART_OLC::Tree));
    for (uint32_t i = 0; i < part_cnt; i++) {
        if(WORKLOAD == TPCH)
            roots[i] = new ART_OLC::Tree{loadKey};
        if(WORKLOAD == CHBench) {
            if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ1) {
                roots[i] = new ART_OLC::Tree{loadKey_chbenchq1};
            }
            if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ6) {
                roots[i] = new ART_OLC::Tree{loadKey_chbenchq6};
            }
        }
        if(CHBENCH_QUERY_TYPE != CHBenchQuery::CHBenchTest)
            assert(roots[i]);
    }
    return RCOK;
}

RC index_art::init(uint64_t part_cnt, table_t * table) {
    this->table = table;
    init(part_cnt);
    return RCOK;
}

int index_art::index_size() {
    int size = 0;

    for (uint64_t i = 0; i < part_cnt; i++) {
        ART_OLC::Tree * root = find_root(i);
        assert(root != NULL);

        ART::ThreadInfo thread_info = root->getThreadInfo();
        size += root->size(thread_info);
    }

    return size;
}

ART_OLC::Tree * index_art::find_root(uint64_t part_id) {
    assert(part_id < part_cnt);
    return roots[part_id];
}

bool index_art::index_exist(idx_key_t key) {
    ART_OLC::Tree * root = find_root(0);
    assert(root != NULL);

    ART::ThreadInfo thread_info = root->getThreadInfo();
    Key k;
    k.setInt(key);
    TID tid = root->lookup(k, thread_info);
    
    return tid > 0;
}

bool index_art::index_exist(idx_key_t key, int part_id) {
    ART_OLC::Tree * root = find_root(part_id);
    assert(root != NULL);

    ART::ThreadInfo thread_info = root->getThreadInfo();
    Key k;
    k.setInt(key);
    TID tid = root->lookup(k, thread_info);
    
    return tid > 0;
}

RC index_art::index_insert(idx_key_t key, itemid_t * item, int part_id) {
    ART_OLC::Tree * root = find_root(part_id);
    assert(root != NULL);

    ART::ThreadInfo thread_info = root->getThreadInfo();
    Key k;
    k.setInt(key);
    root->insert(k, (TID)item, thread_info);
    return RCOK;
}


RC index_art::index_read(idx_key_t key, itemid_t * &item, int part_id) {
    ART_OLC::Tree * root = find_root(part_id);
    assert(root != NULL);

    ART::ThreadInfo thread_info = root->getThreadInfo();
    Key k;
    k.setInt(key);
    TID tid = root->lookup(k, thread_info);
    item = reinterpret_cast<itemid_t *>(tid);
    return RCOK;
}

RC index_art::index_read(idx_key_t key, itemid_t * &item, int part_id, int thd_id) {
    ART_OLC::Tree * root = find_root(part_id);
    assert(root != NULL);

    ART::ThreadInfo thread_info = root->getThreadInfo();
    Key k;
    k.setInt(key);
    TID tid = root->lookup(k, thread_info);
    item = reinterpret_cast<itemid_t *>(tid);
    return RCOK;
}

RC index_art::index_remove(idx_key_t key) {
    ART_OLC::Tree * root = find_root(0);
    assert(root != NULL);

    ART::ThreadInfo thread_info = root->getThreadInfo();
    Key k;
    k.setInt(key);
    root->remove(k, thread_info);
    return RCOK;
}

bool index_art::lookup_range(idx_key_t start, idx_key_t end, int part_id, itemid_t **result, std::size_t resultSize, std::size_t &resultfound)
{
    ART_OLC::Tree * root = find_root(part_id);
    assert(root != NULL);
    ART::ThreadInfo thread_info = root->getThreadInfo();
    Key startKey, endKey;
    startKey.setInt(start);
    endKey.setInt(end);
    Key tmp;

    // resultSize is not enough
    assert(root->lookupRange(startKey, endKey, tmp, reinterpret_cast<TID *>(result), resultSize, resultfound, thread_info) == 0);
    return true;
}
