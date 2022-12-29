#include "global.h"
#include "helper.h"
#include "tpc_helper.h"
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

RC tpch_wl::init() 
{
#if TPCH_EVA_CUBIT
	init_bitmap();
#endif
	workload::init();
	string path = "./benchmarks/";
	path += "TPCH_schema.txt";
	cout << "reading TPCH schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "TPCH schema initialized" << endl;
	t_lineitem->init_row_buffer(g_max_lineitems);
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
	i_Q6_hashtable = indexes["Q6_IDX"];
	return RCOK;
}

RC tpch_wl::init_table() {
	tpc_buffer = new drand48_data * [1];
	tpc_buffer[0] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	int tid = ATOM_FETCH_ADD(next_tid, 1);
	assert(tid == 0);
	srand48_r(1, tpc_buffer[tid]);

	init_tab_orderAndLineitem();

	printf("TPCH Data Initialization Complete!\n");
	return RCOK;
}

RC tpch_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (tpch_txn_man *) _mm_malloc( sizeof(tpch_txn_man), 64);
	new(txn_manager) tpch_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

void tpch_wl::init_tab_orderAndLineitem() {
	cout << "initializing ORDER and LINEITEM table" << endl;	
	for (uint64_t i = 1; i <= g_num_orders; ++i) {
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
		g_nor_in_lineitems += lines;
		for (uint64_t lcnt = 1; lcnt <= lines; lcnt++) {
			row_t * row2;
			uint64_t row_id2 = 0;
			// t_lineitem->get_new_row(row2, 0, row_id2);
			t_lineitem->get_new_row_seq(row2, 0, row_id2);

			// Populate data
			// Primary key
			row2->set_primary_key(tpch_lineitemKey(i, lcnt));
			row2->set_value(L_ORDERKEY, i);
			row2->set_value(L_LINENUMBER, lcnt);

			// Related data
			double quantity = (double)URand(1, 50, 0);
			row2->set_value(L_QUANTITY, quantity); 
			uint64_t partkey = URand(1, SF * 200000, 0);	// Related to SF
			row2->set_value(L_PARTKEY, partkey); 
			uint64_t p_retailprice = (uint64_t)((90000 + ((partkey / 10) % 20001) + 100 *(partkey % 1000)) /100); // Defined in table PART
			row2->set_value(L_EXTENDEDPRICE, (double)(quantity * p_retailprice)); // 
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
			// cout << "L_QUANTITY " << quantity<< endl;	
			// cout << "L_PARTKEY " << partkey<< endl;	
			// cout << "L_EXTENDEDPRICE " << (double)(quantity * p_retailprice) << endl;	
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
			uint64_t Q6_key = tpch_lineitemKey_index(shipdate, discount, (uint64_t)quantity);
			// cout << "Q6_insert_key = " << Q6_key << endl; 
			index_insert(i_Q6_hashtable, Q6_key, row2, 0);

#if TPCH_EVA_CUBIT
			if (bitmap_shipdate->config->approach == "naive" ) {
				bitmap_shipdate->append(0, row_id2);
			}
			else if (bitmap_shipdate->config->approach == "nbub-lk") {
				nbub::Nbub *bitmap = dynamic_cast<nbub::Nbub *>(bitmap_shipdate);
				bitmap->__init_append(0, row_id2, (shipdate/1000-92));

				bitmap = dynamic_cast<nbub::Nbub *>(bitmap_discount);
				bitmap->__init_append(0, row_id2, discount);

				bitmap = dynamic_cast<nbub::Nbub *>(bitmap_quantity);
				bitmap->__init_append(0, row_id2, quantity-1);
			}
#endif
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
		// t_lineitem->get_new_row(row2, 0, row_id2);
		t_lineitem->get_new_row_seq(row2, 0, row_id2);
		// Primary key
		// uint64_t i = 1;
		uint64_t lcnt = 1;
		// uint64_t key1 = (uint64_t)(i * 8 + lcnt);
		row2->set_primary_key(i);
		row2->set_value(L_ORDERKEY, i);
		row2->set_value(L_LINENUMBER, lcnt);

		// Related data
		double quantity = (double)23;
		double exprice = (double)100;
		double discount = (double)0.06;
		uint64_t shipdate = (uint64_t) 96221;
		row2->set_value(L_QUANTITY, quantity); 	
		row2->set_value(L_EXTENDEDPRICE, exprice); 
		row2->set_value(L_DISCOUNT, discount);
		row2->set_value(L_SHIPDATE,shipdate );	

		//Index 
		// index_insert(i_lineitem, tpch_lineitemKey(i,lcnt), row2, 0);
		index_insert(i_lineitem, i, row2, 0);
		
		// Q6 index
		// uint64_t Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
		// index_insert(i_Q6_hashtable, Q6_key, row2, 0);
	}
	uint64_t toDeleteKey = (uint64_t)1;
	i_lineitem->index_remove(toDeleteKey);
	row_t * row2;
	uint64_t row_id2;
	// t_lineitem->get_new_row(row2, 0, row_id2);		
	t_lineitem->get_new_row_seq(row2, 0, row_id2);
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
	// quantity = (double)23;
	// exprice = (double)100;
	// discount = (double)0.06;
	// shipdate = (uint64_t) 96222;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	//Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);
		
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
	// quantity = (double)23;
	// exprice = (double)100;
	// discount = (double)0.06;
	// shipdate = (uint64_t) 96223;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);


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
	// quantity = (double)27;
	// exprice = (double)100;
	// discount = (double)0.03;
	// shipdate = (uint64_t) 92132;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);

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
	// quantity = (double)20;
	// exprice = (double)100;
	// discount = (double)0.04;
	// shipdate = (uint64_t) 95222;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);

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
	// quantity = (double)10;
	// exprice = (double)100;
	// discount = (double)0.05;
	// shipdate = (uint64_t) 95365;
	// row2->set_value(L_QUANTITY, quantity); 	
	// row2->set_value(L_EXTENDEDPRICE, exprice); 
	// row2->set_value(L_DISCOUNT, discount);
	// row2->set_value(L_SHIPDATE,shipdate );	
	
	// //Index 
	// index_insert(i_lineitem, key1, row2, 0);
	// // Q6 index
	// Q6_key = (uint64_t)((shipdate * 12 + (uint64_t)(discount*100)) * 26 + (uint64_t)quantity); 
	// index_insert(i_Q6_hashtable, Q6_key, row2, 0);
}


RC tpch_wl::init_bitmap() 
{

/********************* bitmap_shipdate ******************************/
	{
	Table_config *config_shipdate = new Table_config{};
	config_shipdate->n_workers = g_thread_cnt;
	config_shipdate->DATA_PATH = "";
	config_shipdate->INDEX_PATH = "";
	config_shipdate->g_cardinality = 7; // [92, 98]
	enable_fence_pointer = config_shipdate->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_shipdate->approach = {"nbub-lk"};
	config_shipdate->nThreads_for_getval = 4;
	config_shipdate->show_memory = true;
	config_shipdate->on_disk = false;
	config_shipdate->showEB = false;
    config_shipdate->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_shipdate->n_rows = 0; 
	config_shipdate->n_queries = 900;
	config_shipdate->n_deletes = 100;
	config_shipdate->n_merge = 16;
	config_shipdate->verbose = false;
	config_shipdate->time_out = 100;
	
	if (config_shipdate->approach == "ub") {
        bitmap_shipdate = new ub::Table(config_shipdate);
    } else if (config_shipdate->approach == "nbub-lk") {
        bitmap_shipdate = new nbub_lk::NbubLK(config_shipdate);
    } else if (config_shipdate->approach == "nbub-lf" || config_shipdate->approach =="nbub") {
        bitmap_shipdate = new nbub_lf::NbubLF(config_shipdate);
    } else if (config_shipdate->approach == "ucb") {
        bitmap_shipdate = new ucb::Table(config_shipdate);
    } else if (config_shipdate->approach == "naive") {
        bitmap_shipdate = new naive::Table(config_shipdate);
    }
    else {
        cerr << "Unknown approach." << endl;
        exit(-1);
    }

	cout << "[CUBIT]: Bitmap bitmap_shipdate initialized successfully. "
			<< "[Cardinality:" << config_shipdate->g_cardinality
			<< "] [Method:" << config_shipdate->approach << "]" << endl;
	}

/********************* bitmap_discount ******************************/
	{
	Table_config *config_discount = new Table_config{};
	config_discount->n_workers = g_thread_cnt;
	config_discount->DATA_PATH = "";
	config_discount->INDEX_PATH = "";
	config_discount->g_cardinality = 11; // [0, 10]
	enable_fence_pointer = config_discount->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_discount->approach = {"nbub-lk"};
	config_discount->nThreads_for_getval = 4;
	config_discount->show_memory = true;
	config_discount->on_disk = false;
	config_discount->showEB = false;
    config_discount->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_discount->n_rows = 0; 
	config_discount->n_queries = 900;
	config_discount->n_deletes = 100;
	config_discount->n_merge = 16;
	config_discount->verbose = false;
	config_discount->time_out = 100;
	
	if (config_discount->approach == "ub") {
        bitmap_discount = new ub::Table(config_discount);
    } else if (config_discount->approach == "nbub-lk") {
        bitmap_discount = new nbub_lk::NbubLK(config_discount);
    } else if (config_discount->approach == "nbub-lf" || config_discount->approach =="nbub") {
        bitmap_discount = new nbub_lf::NbubLF(config_discount);
    } else if (config_discount->approach == "ucb") {
        bitmap_discount = new ucb::Table(config_discount);
    } else if (config_discount->approach == "naive") {
        bitmap_discount = new naive::Table(config_discount);
    }
    else {
        cerr << "Unknown approach." << endl;
        exit(-1);
    }

	cout << "[CUBIT]: Bitmap bitmap_discount initialized successfully. "
			<< "[Cardinality:" << config_discount->g_cardinality
			<< "] [Method:" << config_discount->approach << "]" << endl;
	}

/********************* bitmap_quantity ******************************/
	{
	Table_config *config_quantity = new Table_config{};
	config_quantity->n_workers = g_thread_cnt;
	config_quantity->DATA_PATH = "";
	config_quantity->INDEX_PATH = "";
	config_quantity->g_cardinality = 50; // [0, 49]
	enable_fence_pointer = config_quantity->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_quantity->approach = {"nbub-lk"};
	config_quantity->nThreads_for_getval = 4;
	config_quantity->show_memory = true;
	config_quantity->on_disk = false;
	config_quantity->showEB = false;
    config_quantity->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_quantity->n_rows = 0; 
	config_quantity->n_queries = 900;
	config_quantity->n_deletes = 100;
	config_quantity->n_merge = 16;
	config_quantity->verbose = false;
	config_quantity->time_out = 100;
	
	if (config_quantity->approach == "ub") {
        bitmap_quantity = new ub::Table(config_quantity);
    } else if (config_quantity->approach == "nbub-lk") {
        bitmap_quantity = new nbub_lk::NbubLK(config_quantity);
    } else if (config_quantity->approach == "nbub-lf" || config_quantity->approach =="nbub") {
        bitmap_quantity = new nbub_lf::NbubLF(config_quantity);
    } else if (config_quantity->approach == "ucb") {
        bitmap_quantity = new ucb::Table(config_quantity);
    } else if (config_quantity->approach == "naive") {
        bitmap_quantity = new naive::Table(config_quantity);
    }
    else {
        cerr << "Unknown approach." << endl;
        exit(-1);
    }

	cout << "[CUBIT]: Bitmap bitmap_quantity initialized successfully. "
			<< "[Cardinality:" << config_quantity->g_cardinality
			<< "] [Method:" << config_quantity->approach << "]" << endl;
	}

	return RCOK;
}