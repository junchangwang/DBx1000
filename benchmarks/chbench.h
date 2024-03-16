#ifndef _CHBENCH_H_
#define _CHBENCH_H_

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

#define CHBENCH_Q6_SCAN_THREADS 4

class table_t;
class INDEX;
class chbench_query;

class chbench_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_bitmap(); 
	RC init_bitmap_c_w_id();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	table_t * 		t_warehouse;
	table_t * 		t_district;
	table_t * 		t_customer;
	table_t *		t_history;
	table_t *		t_neworder;
	table_t *		t_order;
	table_t *		t_orderline;
	table_t *		t_item;
	table_t *		t_stock;

	INDEX * 	i_item;
	INDEX * 	i_warehouse;
	INDEX * 	i_district;
	INDEX * 	i_customer_id;
	INDEX * 	i_customer_last;
	INDEX *		i_customers;
	INDEX * 	i_stock;
	INDEX * 	i_order; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline; // key = (quantity, data)
	INDEX * 	i_orderline_d; // key = (data). 

	BaseTable *bitmap_c_w_id;
	BaseTable *bitmap_deliverydate;
	BaseTable *bitmap_quantity;
	BaseTable *bitmap_q1;
	BaseTable *bitmap_ol_number;

	bool ** delivering;
	uint32_t next_tid;
private:
	uint64_t num_wh;
	void init_tab_item();
	void init_tab_wh(uint32_t wid);
	void init_tab_dist(uint64_t w_id);
	void init_tab_stock(uint64_t w_id);
	void init_tab_cust(uint64_t d_id, uint64_t w_id);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
	void init_tab_order(uint64_t d_id, uint64_t w_id);
	
	void init_permutation(uint64_t * perm_c_id, uint64_t wid);

	static void * threadInitItem(void * This);
	static void * threadInitWh(void * This);
	static void * threadInitDist(void * This);
	static void * threadInitStock(void * This);
	static void * threadInitCust(void * This);
	static void * threadInitHist(void * This);
	static void * threadInitOrder(void * This);

	static void * threadInitWarehouse(void * This);
	static void * threadInitWarehouse_sequential(void * This);
};


class chbench_q1_data {

public:
	chbench_q1_data(int size);
	chbench_q1_data(const chbench_q1_data& obj);
	int d_size;
	uint64_t *sum_quantity;
	int *cnt;
	double *sum_amount;
	double *avg_amount;
	double *avg_quantity;
	void operator+=(const chbench_q1_data& tmp);
	~chbench_q1_data();
};


class chbench_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(int tid, base_query * query);
private:
	chbench_wl * _wl;
	RC run_payment(chbench_query * m_query);
	RC run_new_order(chbench_query * m_query);
	RC run_order_status(chbench_query * query);
	RC run_delivery(chbench_query * query);
	RC run_stock_level(chbench_query * query);
	RC evaluate_index(chbench_query *query);
	void run_Q6_scan_singlethread(uint64_t start_row, uint64_t end_row, chbench_query * query, std::tuple<double, int> &result);
	void q1_add_answer(row_t* row_local, chbench_q1_data &result);
	void run_Q1_scan_singlethread(uint64_t start_row, uint64_t end_row, chbench_query * query, chbench_q1_data &result);
	RC run_Q6_scan(int tid, chbench_query * query);
	RC run_Q6_btree(int tid, chbench_query * query);
	RC run_Q6_bitmap(int tid, chbench_query * query);
	RC run_Q1_scan(int tid, chbench_query * query);
	RC run_Q1_btree(int tid, chbench_query * query);
	RC run_Q1_bitmap(int tid, chbench_query * query);
	void run_Q1_bitmap_fetch_singlethread(int number, nbub::Nbub *bitmap_d, nbub::Nbub *bitmap_number, chbench_q1_data & ans);
	RC run_Q1_bitmap_parallel_fetch(int tid, chbench_query * query);
};


static inline int bitmap_quantity_bin(int64_t quantity) {
	// [<1], [1,1000], [>1000]
	if (quantity < 1)
		return 0;
	else if (quantity > 1000)
		return 2;
	else return 1;
}

	// [<1999], [1999,2020), [>=2020]
	//   0          1            2     
static inline int bitmap_deliverydate_bin(uint64_t date) {
	if (date < 1999)
		return 0;
	else if (date >= 2020)
		return 2;
	else return 1;
}

#endif
