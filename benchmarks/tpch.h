#ifndef _TPCH_H_
#define _TPCH_H_

#include "wl.h"
#include "txn.h"

class table_t;
class INDEX;
class tpcc_query;

class tpch_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	table_t * 		t_lineitem;
	table_t * 		t_orders;

	INDEX * 	i_orders;
	INDEX * 	i_lineitem;
	
	bool ** delivering;
	uint32_t next_tid;
private:
	uint64_t num_wh;
	void init_tab_lineitem();
	void init_tab_order(uint32_t wid);
	

	void init_permutation(uint64_t * perm_c_id, uint64_t wid);

	static void * threadInitWarehouse(void * This);
};

class tpch_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query);
private:
	tpch_wl * _wl;
	RC run_payment(tpcc_query * m_query);
	RC run_new_order(tpcc_query * m_query);
	RC run_order_status(tpcc_query * query);
	RC run_delivery(tpcc_query * query);
	RC run_stock_level(tpcc_query * query);
};

#endif
