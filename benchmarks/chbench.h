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
#include "cubit/table_lf.h"
#include "cubit/table_lk.h"

#define CHBENCH_Q6_SCAN_THREADS 4
#define CHBENCH_Q1_MIN_DELIVERY_DATE 20070102

class table_t;
class INDEX;
class chbench_query;

class chbench_wl : public workload {
public:
	RC init();
	RC build();
	RC init_table();
	RC init_bitmap(); 
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
	table_t *		t_supplier;
	table_t *		t_nation;
	table_t *		t_region;

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
	INDEX *		i_supplier;
	INDEX *		i_nation;
	INDEX *		i_region;

	BaseTable *bitmap_q6_deliverydate;
	BaseTable *bitmap_q6_quantity;
	BaseTable *bitmap_q1_deliverydate;
	BaseTable *bitmap_q1_ol_number;
	index_bwtree *	i_Q6_bwtree;
	index_art *		i_Q6_art;
	index_bwtree *	i_Q1_bwtree;
	index_art *		i_Q1_art;

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
	void init_tab_supplier();
	void init_tab_nation();
	void init_tab_region();
	void tree_insert(INDEX *tree_q1, INDEX *tree_q6, uint64_t key_q1, uint64_t key_q6, row_t *row, uint64_t primary_key);
	
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
	void operator=(const chbench_q1_data& tmp);
	~chbench_q1_data();
};


class chbench_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(int tid, base_query * query);
private:
	chbench_wl * _wl;
	RC run_payment(int tid, chbench_query * m_query);
	RC run_new_order(int tid, chbench_query * m_query);
	RC run_order_status(chbench_query * query);
	RC run_delivery(chbench_query * query);
	RC run_stock_level(chbench_query * query);
	RC evaluate_index(chbench_query *query);
	void run_Q6_scan_singlethread(uint64_t start_row, uint64_t end_row, chbench_query * query, std::tuple<double, int> &result, int wid);
	void q1_add_answer(row_t* row_local, chbench_q1_data &result);
	void run_Q1_scan_singlethread(uint64_t start_row, uint64_t end_row, chbench_query * query, chbench_q1_data &result, int wid);
	RC run_Q6_scan(int tid, chbench_query * query);
	RC run_Q6_btree(int tid, chbench_query * query);
	RC run_Q6_bwtree(int tid, chbench_query * query);
	RC run_Q6_art(int tid, chbench_query * query);
	RC run_Q6_bitmap(int tid, chbench_query * query);
	void bitmap_singlethread_fetch(int begin, int end, pair<double, int> &result, row_t *row_buffer, int *ids, int wid);
	void run_Q6_bitmap_singlethread(SegBtv &seg_btv1, SegBtv &seg_btv2, int begin, int end, pair<double, int> &result);
	RC run_Q6_bitmap_parallel(int tid, chbench_query * query);
	void get_bitvector_result(ibis::bitvector &result, cubit::Cubit *cubit1, cubit::Cubit *cubit2, int cubit1_pos, int cubit2_pos);
	RC run_Q1_scan(int tid, chbench_query * query);
	RC run_Q1_btree(int tid, chbench_query * query);
	RC run_Q1_bitmap(int tid, chbench_query * query);
	void run_Q1_bitmap_fetch_singlethread(int number, cubit::Cubit *bitmap_d, cubit::Cubit *bitmap_number, chbench_q1_data & ans, int wid);
	RC run_Q1_bitmap_parallel_fetch(int tid, chbench_query * query);
	RC run_Q1_bwtree(int tid, chbench_query * query);
	RC run_Q1_art(int tid, chbench_query * query);
	RC run_test_table(int tid,chbench_query * query);
};


static inline int bitmap_quantity_bin(int64_t quantity) {
	// [<1], [1,1000], [>1000]
	if (quantity < 1)
		return 0;
	else if (quantity > 1000)
		return 2;
	else return 1;
}
  
  
static inline int bitmap_deliverydate_bin(uint64_t date) {
	// [<19990101], [19990101,20200101), [>=20200101] 
	if (date < 19990101)
		return 0;
	else if (date >= 20200101)
		return 2;
	else return 1;
}

#endif
