#include "global.h"
#include "helper.h"
#include "tpcc_helper.h"
#include "tpch.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
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
	i_Q6_index = indexes["Q6_IDX"];
	return RCOK;
}

RC tpch_wl::init_table() {
	num_wh = g_num_wh;
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
	txn_manager = (tpch_txn_man *) _mm_malloc( sizeof(tpch_txn_man), 64);
	new(txn_manager) tpch_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

void tpch_wl::init_tab_lineitem() {
	cout << "initializing LINEITEM table" << endl;
	g_total_line_in_lineitems += g_max_lineitem ;
	for (uint64_t i = 1; i <= g_max_lineitem; i++) {
		uint64_t lines = URand(1, 7, 0);
		g_total_line_in_lineitems += lines;
		for (uint64_t lcnt = 1; lcnt <= lines; lcnt++) {
			row_t * row;
			uint64_t row_id;
			t_lineitem->get_new_row(row, 0, row_id);

			// Populate data
			// Primary key
			row->set_primary_key(i * 8 + lcnt);
			row->set_value(L_ORDERKEY, i);
			row->set_value(L_LINENUMBER, lcnt);

			// Related data
			row->set_value(L_EXTENDEDPRICE, (double)URand(0, 1000, 0)); // To be fixed
			row->set_value(L_DISCOUNT, ((double)URand(0, 10, 0)) / 100); 
			
			row->set_value(L_SHIPDATE, URand(1990, 2020, 0));	 // To be fixed, O_ORDERDATE + random value[1.. 121]
			// O_ORDERDATE(in table ORDER)should be chosen randomly from [1992.01.01-1998.12.31]. 
			row->set_value(L_QUANTITY, (double)URand(1, 50, 0)); 
			row->set_value(L_PARTKEY, (uint64_t)123456); //May be used 

			//Unrelated data
			row->set_value(L_SUPPKEY, (uint64_t)123456);
			row->set_value(L_TAX, (double)URand(0, 8, 0));
			row->set_value(L_RETURNFLAG, 'a');
			row->set_value(L_LINESTATUS, 'b');
			row->set_value(L_COMMITDATE, (uint64_t)2022);
			row->set_value(L_RECEIPTDATE, (uint64_t)2022);
			char temp[20];
			MakeAlphaString(10, 19, temp, 0);
			row->set_value(L_SHIPINSTRUCT, temp);
			row->set_value(L_SHIPMODE, temp);
			row->set_value(L_COMMENT, temp);

			//Index 
			index_insert(i_lineitem, i, row, 0);
		}
	}
	
}

void tpch_wl::init_tab_order() {
	cout << "initializing ORDER table" << endl;	
	for (uint64_t i = 1; i <= g_max_lineitem; ++i) {
		row_t * row;
		uint64_t row_id;
		t_orders->get_new_row(row, 0, row_id);		

		//Primary key
		row->set_primary_key(i);
		row->set_value(O_ORDERKEY, i);

		//Related data
		uint64_t year = URand(92, 98, 0);
		uint64_t day = URand(1, 365, 0);
		if (year == 98) {
			day = day % 214;
		} 
		row->set_value(O_ORDERDATE, (uint64_t)(year*1000 + day)); 

		//Unrelated data
		row->set_value(O_CUSTKEY, (uint64_t)123456);
		row->set_value(O_ORDERSTATUS, 'A');
		row->set_value(O_TOTALPRICE, (double)12345.56); // May be used
		char temp[20];
		MakeAlphaString(10, 19, temp, 0);
		row->set_value(O_ORDERPRIORITY, temp);
		row->set_value(O_CLERK, temp);
		row->set_value(O_SHIPPRIORITY, (uint64_t)654321);
		row->set_value(O_COMMENT, temp);

		index_insert(i_orders, i, row, 0);
	}
}

void tpch_wl::init_tab_orderAndLineitem() {
	cout << "initializing ORDER and LINEITEM table" << endl;	
	for (uint64_t i = 1; i <= g_max_lineitem; ++i) {
		row_t * row;
		uint64_t row_id;
		t_orders->get_new_row(row, 0, row_id);		

		//Primary key
		row->set_primary_key(i);
		row->set_value(O_ORDERKEY, i);

		//Related data
		uint64_t year = URand(92, 98, 0);
		uint64_t day = URand(1, 365, 0);
		if (year == 98) {
			day = day % 214;
		} 
		uint64_t orderdate = (uint64_t)(year*1000 + day);
		row->set_value(O_ORDERDATE, orderdate); 

		//Unrelated data
		row->set_value(O_CUSTKEY, (uint64_t)123456);
		row->set_value(O_ORDERSTATUS, 'A');
		row->set_value(O_TOTALPRICE, (double)12345.56); 
		char temp[20];
		MakeAlphaString(10, 19, temp, 0);
		row->set_value(O_ORDERPRIORITY, temp);
		row->set_value(O_CLERK, temp);
		row->set_value(O_SHIPPRIORITY, (uint64_t)654321);
		row->set_value(O_COMMENT, temp);

		index_insert(i_orders, i, row, 0);





		// **********************Lineitems*****************************************

		uint64_t lines = URand(1, 7, 0);
		// uint64_t lines = 1;
		g_total_line_in_lineitems += lines;
		for (uint64_t lcnt = 1; lcnt <= lines; lcnt++) {
			row_t * row2;
			uint64_t row_id2;
			t_lineitem->get_new_row(row2, 0, row_id2);

			// Populate data
			// Primary key
			row2->set_primary_key(tpch_lineitemKey(i, lcnt));
			row2->set_value(L_ORDERKEY, i);
			row2->set_value(L_LINENUMBER, lcnt);

			// Related data
			double quntity = (double)URand(1, 50, 0);
			row2->set_value(L_QUANTITY, quntity); 
			uint64_t partkey = URand(1, SF * 200000, 0);	// Related to SF
			row2->set_value(L_PARTKEY, partkey); 
			uint64_t p_retailprice = (uint64_t)((90000 + ((partkey / 10) % 20001) + 100 *(partkey % 1000)) /100); // Defined in table PART
			row2->set_value(L_EXTENDEDPRICE, (double)(quntity * p_retailprice)); // 
			uint64_t discount = URand(0, 10, 0); // discount is defined as int for Q6
			row2->set_value(L_DISCOUNT, ((double)discount) / 100); 
			uint64_t shipdate;
			uint64_t dayAdd = URand(1, 121, 0);
			if (day + dayAdd > (uint64_t)365) {
				shipdate = (uint64_t)((year+1) * 1000 + day + dayAdd - 365);
			} else {
				shipdate = (uint64_t) (year *1000 + day + dayAdd);
			}
			row2->set_value(L_SHIPDATE, shipdate);	 //  O_ORDERDATE + random value[1.. 121]
			
			// debug
			// cout << endl << "-----------pupulating data debug " << endl;
			// cout << "O_ORDERKEY " << i << endl;	
			// cout << "O_ORDERDATE " << orderdate << endl ;	
			// cout << "-----------lineitem--- " << endl;
			// cout << "L_QUANTITY " << quntity<< endl;	
			// cout << "L_PARTKEY " << partkey<< endl;	
			// cout << "L_EXTENDEDPRICE " << (double)(quntity * p_retailprice) << endl;	
			// cout << "L_DISCOUNT " << ((double)discount) / 100 << endl;	
			// cout << "L_SHIPDATE " << shipdate << endl;	


			//Unrelated data
			row2->set_value(L_SUPPKEY, (uint64_t)123456);
			row2->set_value(L_TAX, (double)URand(0, 8, 0));
			row2->set_value(L_RETURNFLAG, 'a');
			row2->set_value(L_LINESTATUS, 'b');
			row2->set_value(L_COMMITDATE, (uint64_t)2022);
			row2->set_value(L_RECEIPTDATE, (uint64_t)2022);
			char temp[20];
			MakeAlphaString(10, 19, temp, 0);
			row2->set_value(L_SHIPINSTRUCT, temp);
			row2->set_value(L_SHIPMODE, temp);
			row2->set_value(L_COMMENT, temp);

			//Index 
			uint64_t key = tpch_lineitemKey(i, lcnt);
			index_insert(i_lineitem, key, row2, 0);


			// Q6 index
			uint64_t Q6_key = tpch_lineitemKey_index(shipdate, discount, (uint64_t)quntity);
			// cout << "Q6_insert_key = " << Q6_key << endl; 
			index_insert(i_Q6_index, Q6_key, row2, 0);
		}
	}	
}

void tpch_wl::init_test() {
	cout << "initializing TEST table" << endl;	


	// // ###################1
	// row_t * row;
	// uint64_t row_id;
	// t_orders->get_new_row(row, 0, row_id);		
	// //Primary key
	// row->set_primary_key(1);
	// row->set_value(O_ORDERKEY, 1);
	// index_insert(i_orders, i, row, 0);
	// // ####################2
	// row_t * row;
	// uint64_t row_id;
	// t_orders->get_new_row(row, 0, row_id);		
	// //Primary key
	// row->set_primary_key(2);
	// row->set_value(O_ORDERKEY, 2);
	// index_insert(i_orders, i, row, 0);



		// **********************Lineitems*****************************************
	// ###################################11111111111111
	for (uint64_t i = 1; i <= 10; ++i) {
		row_t * row2;
		uint64_t row_id2;
		t_lineitem->get_new_row(row2, 0, row_id2);
		// Primary key
		// uint64_t i = 1;
		uint64_t lcnt = 1;
		// uint64_t key1 = (uint64_t)(i * 8 + lcnt);
		row2->set_primary_key(i);
		row2->set_value(L_ORDERKEY, i);
		row2->set_value(L_LINENUMBER, lcnt);

		// Related data
		double quntity = (double)23;
		double exprice = (double)100;
		double discount = (double)0.06;
		uint64_t shipdate = (uint64_t) 96221;
		row2->set_value(L_QUANTITY, quntity); 	
		row2->set_value(L_EXTENDEDPRICE, exprice); 
		row2->set_value(L_DISCOUNT, discount);
		row2->set_value(L_SHIPDATE,shipdate );	

		//Index 
		// index_insert(i_lineitem, tpch_lineitemKey(i,lcnt), row2, 0);
		index_insert(i_lineitem, i, row2, 0);
		
		// Q6 index
		// uint64_t Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quntity); 
		// index_insert(i_Q6_index, Q6_key, row2, 0);
	}
	uint64_t toDeleteKey = (uint64_t)1;
	i_lineitem->index_remove(toDeleteKey);
	row_t * row2;
	uint64_t row_id2;
	t_lineitem->get_new_row(row2, 0, row_id2);		
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);
	index_insert(i_lineitem, toDeleteKey, row2, 0);


	// ###################################222222222222222
	// row_t * row2;
	// uint64_t row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// Primary key
	// i = 1000;
	// lcnt = 1;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// Related data
	// quntity = (double)23;
	// exprice = (double)100;
	// discount = (double)0.06;
	// shipdate = (uint64_t) 96222;
	// row2->set_value(L_QUANTITY, quntity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	//Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quntity); 
	// index_insert(i_Q6_index, Q6_key, row2, 0);
		
	// // ###################################333333333333333333
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 1000;
	// lcnt = 2;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quntity = (double)23;
	// exprice = (double)100;
	// discount = (double)0.06;
	// shipdate = (uint64_t) 96223;
	// row2->set_value(L_QUANTITY, quntity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quntity); 
	// index_insert(i_Q6_index, Q6_key, row2, 0);


	// 	// ###################################4444444444444
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 1000;
	// lcnt = 3;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quntity = (double)27;
	// exprice = (double)100;
	// discount = (double)0.03;
	// shipdate = (uint64_t) 92132;
	// row2->set_value(L_QUANTITY, quntity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quntity); 
	// index_insert(i_Q6_index, Q6_key, row2, 0);

	// 	// ###################################55555555555555
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 2002;
	// lcnt = 1;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quntity = (double)20;
	// exprice = (double)100;
	// discount = (double)0.04;
	// shipdate = (uint64_t) 95222;
	// row2->set_value(L_QUANTITY, quntity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quntity); 
	// index_insert(i_Q6_index, Q6_key, row2, 0);

	// 	// #################################666666666666666
	// // row_t * row2;
	// // row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);
	// // Primary key
	// i = 3121;
	// lcnt = 1;
	// key1 = (uint64_t)(i * 8 + lcnt);
	// row2->set_primary_key(key1);
	// row2->set_value(L_ORDERKEY, i);
	// row2->set_value(L_LINENUMBER, lcnt);

	// // Related data
	// quntity = (double)10;
	// exprice = (double)100;
	// discount = (double)0.05;
	// shipdate = (uint64_t) 95365;
	// row2->set_value(L_QUANTITY, quntity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quntity); 
	// index_insert(i_Q6_index, Q6_key, row2, 0);



			
}


void * tpch_wl::threadInitWarehouse(void * This) {
	tpch_wl * wl = (tpch_wl *) This;
	int tid = ATOM_FETCH_ADD(wl->next_tid, 1);
	uint32_t wid = tid + 1;
	tpcc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	assert((uint64_t)tid < g_num_wh);
	srand48_r(wid, tpcc_buffer[tid]);
	
	// wl->init_tab_lineitem();
	// wl->init_tab_order();

	wl->init_tab_orderAndLineitem();
	// wl->init_test();

	return NULL;
}
