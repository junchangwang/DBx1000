#include "global.h"
#include "helper.h"
#include "tpch.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpch_const.h"

RC tpch_wl::init() {
	workload::init();
	string path = "./benchmarks/";
	path += "TPCH_schema.txt";
	cout << "reading TPCH schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCH schema initialized" << endl;
	init_table();
	next_tid = 0;
	return RCOK;
}

RC tpch_wl::init_schema(const char * schema_file) {
	workload::init_schema(schema_file);
	t_lineitem = tables["LINEITEM"];
    t_orders = tables["ORDERS"];

	i_lineitem = indexes["LINEITEM_IDX"];
	i_orders = indexes["ORDERS_IDX"];
	return RCOK;
}

RC tpch_wl::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order 
//		- new order
//		- order line
/**********************************/
	tpcc_buffer = new drand48_data * [g_num_wh];
	pthread_t * p_thds = new pthread_t[g_num_wh - 1];
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
	threadInitWarehouse(this);
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_join(p_thds[i], NULL);

	printf("TPCH Data Initialization Complete!\n");
	return RCOK;
}

RC tpch_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	// txn_manager = (tpcc_txn_man *) _mm_malloc( sizeof(tpcc_txn_man), 64);
	// new(txn_manager) tpcc_txn_man();
	// txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

void tpch_wl::init_tab_lineitem() {
	cout << "initializing LINEITEM table" << endl;
	//uint64_t lines = URand(1, 7, 0);
	uint64_t lines = 1;
	uint64_t max_lineitem = 10000;

	for (uint64_t i = 0; i < max_lineitem; i++) {
		for (uint64_t lcnt = 0; lcnt < lines; lcnt++) {
			row_t * row;
			uint64_t row_id;
			t_lineitem->get_new_row(row, 0, row_id);

			//Populate data
			row->set_primary_key(i);
			row->set_value(L_ORDERKEY, i);
			row->set_value(L_LINENUMBER, lcnt+666);
			row->set_value(L_TAX, URand(0, 8, 0));
			row->set_value(L_EXTENDEDPRICE, URand(0, 1000, 0));
			row->set_value(L_DISCOUNT, URand(0, 10, 0)); 
			row->set_value(L_SHIPDATE, URand(2000, 2022, 0));
			row->set_value(L_QUANTITY,URand(0, 1000, 0));

			//Index 
			index_insert(i_lineitem, i, row, 0);
		}
	}
	
}

void tpch_wl::init_tab_order(uint32_t wid) {
	cout << "initializing ORDER table" << endl;

}

// /*==================================================================+
// | ROUTINE NAME
// | InitPermutation
// +==================================================================*/

// void 
// tpcc_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
// 	uint32_t i;
// 	// Init with consecutive values
// 	for(i = 0; i < g_cust_per_dist; i++) 
// 		perm_c_id[i] = i+1;

// 	// shuffle
// 	for(i=0; i < g_cust_per_dist-1; i++) {
// 		uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
// 		uint64_t tmp = perm_c_id[i];
// 		perm_c_id[i] = perm_c_id[j];
// 		perm_c_id[j] = tmp;
// 	}
// }


// /*==================================================================+
// | ROUTINE NAME
// | GetPermutation
// +==================================================================*/

void * tpch_wl::threadInitWarehouse(void * This) {
	tpch_wl * wl = (tpch_wl *) This;
	int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
	uint32_t wid = tid + 1;
	tpcc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	assert((uint64_t)tid < g_num_wh);
	srand48_r(wid, tpcc_buffer[tid]);
	
	wl->init_tab_lineitem();
	wl->init_tab_order(wid);

	return NULL;
}
