#ifndef _TPCH_H_
#define _TPCH_H_

#include "wl.h"
#include "txn.h"

#include "fastbit/bitvector.h"
#include "nicolas/base_table.h"
#include "nicolas/util.h"
#include "ub/table.h"
#include "ucb/table.h"
#include "naive/table.h"
#include "nbub/table_lf.h"
#include "nbub/table_lk.h"

class table_t;
class INDEX;
class tpch_query;

class tpch_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_bitmap();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	table_t * 		t_lineitem;
	table_t * 		t_orders;

	INDEX * 	i_orders;
	INDEX * 	i_lineitem;
	INDEX * 	i_Q6_index;

	BaseTable *bitmap_shipdate;
	BaseTable *bitmap_discount;
	BaseTable *bitmap_quantity;
	
	bool ** delivering;
	uint32_t next_tid;
private:
	uint64_t num_wh;
	void init_tab_orderAndLineitem();
	void init_test();
	
	void init_permutation(uint64_t * perm_c_id, uint64_t wid);
};

class tpch_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query);
private:
	tpch_wl * _wl;
	
	RC run_Q6(tpch_query * m_query);
	RC run_Q6_index(tpch_query * m_query);
	RC run_RF1();
	RC run_RF2();
};

static inline uint64_t tpch_lineitemKey(uint64_t i, uint64_t lcnt) {
  return (uint64_t)(i * 8 + lcnt);
}

static inline uint64_t tpch_lineitemKey_index(uint64_t shipdate, uint64_t discount, uint64_t quantity) {
	return (uint64_t)((shipdate * 12 + discount) * 52 + quantity);
}

#endif
