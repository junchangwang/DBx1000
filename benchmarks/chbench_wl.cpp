#include "global.h"
#include "helper.h"
#include "chbench.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_bwtree.h"
#include "index_art.h"
#include "tpc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "chbench_const.h"
#include "Date.h"

// record the num of rows that meet q6 condition
static int cnt_q6 = 0;
static int cnt_q1_number1 = 0;
static uint64_t q1_number1_quantity = 0;
CHBenchQuery query_number = CHBENCH_QUERY_TYPE;
mt19937_64 rng[100];

RC chbench_wl::init() 
{
#if CHBENCH_EVA_CUBIT
	init_bitmap();
#endif
	workload::init();
	string path = "./benchmarks/";
#if CHBENCH_SMALL
	path += "CHBENCH_short_schema.txt";
#else
	path += "CHBENCH_full_schema.txt";
#endif
	cout << "reading schema file: " << path << endl;
	init_schema( path.c_str() );
	cout << "CHBENCH schema initialized" << endl;
	t_orderline->init_row_buffer(g_cust_per_dist * g_num_wh * 150 + 51200 * g_thread_cnt * 15);
	t_order->init_row_buffer(g_cust_per_dist * g_num_wh * 150);
	init_table();
	next_tid = 0;

	return RCOK;
}

RC chbench_wl::build()
{	
	// Check whether the index has been built before.
	ifstream doneFlag;
	string path;
	if(query_number == CHBenchQuery::CHBenchQ6)
	path = "bm_chbench_n" + to_string(g_num_wh) + "_q6_done";
	else path = "bm_chbench_n" + to_string(g_num_wh) + "_q1_done";
	doneFlag.open(path);
	if (doneFlag.good()) { 
		cout << "WARNING: The index for " + path + " has been built before. Skip building." << endl;
		doneFlag.close();
		return RCOK;
	}

	init();

	int ret;
	cubit::Cubit *bitmap = nullptr;
	if(query_number == CHBenchQuery::CHBenchQ6) {
	// bitmap_q6_deliverydate
	bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q6_deliverydate);
	for (uint64_t i = 0; i < bitmap_q6_deliverydate->config->g_cardinality; ++i) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q6_deliverydate/");
		string cmd = "mkdir -p ";
		cmd.append(temp);
		ret = system(cmd.c_str());
			 sleep(1);
		temp.append(to_string(i));
		temp.append(".bm");
		bitmap->bitmaps[i]->btv->write(temp.c_str());

		ibis::bitvector * test_btv = new ibis::bitvector();
		test_btv->read(temp.c_str());
		test_btv->adjustSize(0, bitmap->g_number_of_rows);
		assert(*(bitmap->bitmaps[i]->btv) == (*test_btv));
	}

	
	bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q6_quantity);
	for (uint64_t i = 0; i < bitmap_q6_quantity->config->g_cardinality; ++i) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q6_quantity/");
		string cmd = "mkdir -p ";
		cmd.append(temp);
		ret = system(cmd.c_str());
				sleep(1);
		temp.append(to_string(i));
		temp.append(".bm");
		bitmap->bitmaps[i]->btv->write(temp.c_str());

		ibis::bitvector * test_btv = new ibis::bitvector();
		test_btv->read(temp.c_str());
		test_btv->adjustSize(0, bitmap->g_number_of_rows);
		assert(*(bitmap->bitmaps[i]->btv) == (*test_btv));
	}
	}
	else {
	bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q1_deliverydate);
	for (uint64_t i = 0; i < bitmap_q1_deliverydate->config->g_cardinality; ++i) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q1_deliverydate/");
		string cmd = "mkdir -p ";
		cmd.append(temp);
		ret = system(cmd.c_str());
				sleep(1);
		temp.append(to_string(i));
		temp.append(".bm");
		bitmap->bitmaps[i]->btv->write(temp.c_str());

		ibis::bitvector * test_btv = new ibis::bitvector();
		test_btv->read(temp.c_str());
		test_btv->adjustSize(0, bitmap->g_number_of_rows);
		assert(*(bitmap->bitmaps[i]->btv) == (*test_btv));
	}

	
	bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q1_ol_number);
	for (uint64_t i = 0; i < bitmap_q1_ol_number->config->g_cardinality; ++i) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q1_ol_number/");
		string cmd = "mkdir -p ";
		cmd.append(temp);
		ret = system(cmd.c_str());
			 sleep(1);
		temp.append(to_string(i));
		temp.append(".bm");
		bitmap->bitmaps[i]->btv->write(temp.c_str());

		ibis::bitvector * test_btv = new ibis::bitvector();
		test_btv->read(temp.c_str());
		test_btv->adjustSize(0, bitmap->g_number_of_rows);
		assert(*(bitmap->bitmaps[i]->btv) == (*test_btv));
	}
	}
	// create done file
	fstream done;   
	done.open(path, ios::out);
	if (done.is_open()) {
		done << bitmap->g_number_of_rows;
		cout << "Succeeded in building bitmap files and " << path << endl;
		done.close(); 
	}
	else {
		cout << "Failed in building bitmap files and " << path << endl;
	} 
	return RCOK; 	
}

RC chbench_wl::init_schema(const char * schema_file) {
	workload::init_schema(schema_file);
	t_warehouse = tables["WAREHOUSE"];
	t_district = tables["DISTRICT"];
	t_customer = tables["CUSTOMER"];
	t_history = tables["HISTORY"];
	t_neworder = tables["NEW-ORDER"];
	t_order = tables["ORDER"];
	t_orderline = tables["ORDER-LINE"];
	t_item = tables["ITEM"];
	t_stock = tables["STOCK"];
	t_supplier=tables["SUPPLIER"];
	t_nation=tables["NATION"];
	t_region=tables["REGION"];

	i_item = indexes["ITEM_IDX"];
	i_warehouse = indexes["WAREHOUSE_IDX"];
	i_district = indexes["DISTRICT_IDX"];
	i_customer_last = indexes["CUSTOMER_LAST_IDX"];
	i_customer_id = indexes["CUSTOMER_ID_IDX"];
	i_customers = indexes["CUSTOMERS_IDX"];
	i_stock = indexes["STOCK_IDX"];
	i_orderline = indexes["ORDERLINE_IDX"];
	i_orderline_d = indexes["ORDERLINE_IDX_D"];
	i_supplier=indexes["SUPPLIER_IDX"];
	i_nation=indexes["NATION_IDX"];
	i_region=indexes["REGION_IDX"];

	string tname("ORDER-LINE");
	int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;

	i_Q6_bwtree = (index_bwtree *) _mm_malloc(sizeof(index_bwtree), 64);
	new(i_Q6_bwtree) index_bwtree();
	i_Q6_bwtree->init(part_cnt, tables[tname]);

	i_Q1_bwtree = (index_bwtree *) _mm_malloc(sizeof(index_bwtree), 64);
	new(i_Q1_bwtree) index_bwtree();
	i_Q1_bwtree->init(part_cnt, tables[tname]);

	i_Q6_art = (index_art *) _mm_malloc(sizeof(index_art), 64);
	new(i_Q6_art) index_art();
	i_Q6_art->init(part_cnt, tables[tname]);

	i_Q1_art = (index_art *) _mm_malloc(sizeof(index_art), 64);
	new(i_Q1_art) index_art();
	i_Q1_art->init(part_cnt, tables[tname]);

	return RCOK;
}

RC chbench_wl::init_table() {
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
	tpc_buffer = new drand48_data * [g_num_wh];
	pthread_t * p_thds = new pthread_t[g_num_wh - 1];
	
	i_Q6_bwtree->UpdateThreadLocal(g_num_wh > g_thread_cnt ? g_num_wh : g_thread_cnt);
	i_Q1_bwtree->UpdateThreadLocal(g_num_wh > g_thread_cnt ? g_num_wh : g_thread_cnt);
	
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitWarehouse, this);
	threadInitWarehouse(this);
	for (uint32_t i = 0; i < g_num_wh - 1; i++) 
		pthread_join(p_thds[i], NULL);

	// threadInitWarehouse_sequential(this);

	printf("CHBENCH Data Initialization Complete!\n");
	cout << "q6 should be " << cnt_q6 << endl;
	cout << "q1 number1 should be " << cnt_q1_number1 << endl;
	cout << "q1 number1 quantity should be " << q1_number1_quantity << endl;
	return RCOK;
}

RC chbench_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd) {
	txn_manager = (chbench_txn_man *) _mm_malloc( sizeof(chbench_txn_man), 64);
	new(txn_manager) chbench_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}

// TODO ITEM table is assumed to be in partition 0
void chbench_wl::init_tab_item() {
	for (UInt32 i = 1; i <= g_max_items; i++) {
		row_t * row;
		uint64_t row_id;
		t_item->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(I_ID, i);
		row->set_value(I_IM_ID, URand(1L,10000L, 0));
		char name[24];
		MakeAlphaString(14, 24, name, 0);
		row->set_value(I_NAME, name);
		row->set_value(I_PRICE, URand(1, 100, 0));
		char data[50];
		MakeAlphaString(26, 50, data, 0);
		// TODO in TPCC, "original" should start at a random position
		if (RAND(10, 0) == 0) 
			strcpy(data, "original");		
		row->set_value(I_DATA, data);
		
		index_insert(i_item, i, row, 0);
	}
}

void chbench_wl::init_tab_wh(uint32_t wid) {
	assert(wid >= 1 && wid <= g_num_wh);
	row_t * row;
	uint64_t row_id;
	t_warehouse->get_new_row(row, 0, row_id);
	row->set_primary_key(wid);

	row->set_value(W_ID, wid);
	char name[10];
	MakeAlphaString(6, 10, name, wid-1);
	row->set_value(W_NAME, name);
	char street[20];
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_1, street);
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_STREET_2, street);
	MakeAlphaString(10, 20, street, wid-1);
	row->set_value(W_CITY, street);
	char state[2];
	MakeAlphaString(2, 2, state, wid-1); /* State */
	row->set_value(W_STATE, state);
	char zip[9];
   	MakeNumberString(9, 9, zip, wid-1); /* Zip */
	row->set_value(W_ZIP, zip);
   	double tax = (double)URand(0L,200L,wid-1)/1000.0;
   	double w_ytd=300000.00;
	row->set_value(W_TAX, tax);
	row->set_value(W_YTD, w_ytd);
	
	index_insert(i_warehouse, wid, row, wh_to_part(wid));
	return;
}

void chbench_wl::init_tab_dist(uint64_t wid) {
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		row_t * row;
		uint64_t row_id;
		t_district->get_new_row(row, 0, row_id);
		row->set_primary_key(did);
		
		row->set_value(D_ID, did);
		row->set_value(D_W_ID, wid);
		char name[10];
		MakeAlphaString(6, 10, name, wid-1);
		row->set_value(D_NAME, name);
		char street[20];
		MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_1, street);
		MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_STREET_2, street);
		MakeAlphaString(10, 20, street, wid-1);
		row->set_value(D_CITY, street);
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(D_STATE, state);
		char zip[9];
		MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(D_ZIP, zip);
		double tax = (double)URand(0L,200L,wid-1)/1000.0;
		double w_ytd=30000.00;
		row->set_value(D_TAX, tax);
		row->set_value(D_YTD, w_ytd);
		row->set_value(D_NEXT_O_ID, static_cast<int64_t>(3001));
		
		index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));
	}
}

void chbench_wl::init_tab_stock(uint64_t wid) {
	
	for (UInt32 sid = 1; sid <= g_max_items; sid++) {
		row_t * row;
		uint64_t row_id;
		t_stock->get_new_row(row, 0, row_id);
		row->set_primary_key(sid);
		row->set_value(S_I_ID, sid);
		row->set_value(S_W_ID, wid);
		row->set_value(S_QUANTITY, URand(10, 100, wid-1));
		row->set_value(S_REMOTE_CNT, 0);
#if !CHBENCH_SMALL
		char s_dist[25];
		char row_name[10] = "S_DIST_";
		for (int i = 1; i <= 10; i++) {
			if (i < 10) {
				row_name[7] = '0';
				row_name[8] = i + '0';
			} else {
				row_name[7] = '1';
				row_name[8] = '0';
			}
			row_name[9] = '\0';
			MakeAlphaString(24, 24, s_dist, wid-1);
			row->set_value(row_name, s_dist);
		}
		row->set_value(S_YTD, 0);
		row->set_value(S_ORDER_CNT, 0);
		char s_data[50];
		int len = MakeAlphaString(26, 50, s_data, wid-1);
		if (rand() % 100 < 10) {
			int idx = URand(0, len - 8, wid-1);
			strcpy(&s_data[idx], "original");
		}
		row->set_value(S_DATA, s_data);
#endif
		index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));
	}
}

void * chbench_wl::threadInitWarehouse_sequential(void * This) {
		chbench_wl * wl = (chbench_wl *) This;

		for (int tid = 0; tid < g_num_wh; tid++) {
				uint32_t wid = tid + 1;
				tpc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
				assert((uint64_t)tid < g_num_wh);
				srand48_r(wid, tpc_buffer[tid]);

				if (tid == 0) {
						wl->init_tab_item();
						// Thread 0 initialize the table Stock to avoid using latches in initializing.
						wl->t_stock->init_row_buffer(g_max_items * g_num_wh);
				}
				wl->init_tab_wh( wid );
				wl->init_tab_dist( wid );
				wl->init_tab_stock(wid);
				for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
						wl->init_tab_cust(did, wid);
						wl->init_tab_order(did, wid);
						for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++)
								wl->init_tab_hist(cid, did, wid);
				}
		}

		return NULL;
}

void chbench_wl::init_tab_cust(uint64_t did, uint64_t wid) {
	assert(g_cust_per_dist >= 1000);
	for (UInt32 cid = 1; cid <= g_cust_per_dist; cid++) {
		row_t * row;
		uint64_t row_id;
		t_customer->get_new_row(row, 0, row_id);
		row->set_primary_key(cid);

		row->set_value(C_ID, cid);		
		row->set_value(C_D_ID, did);
		row->set_value(C_W_ID, wid);
		char c_last[LASTNAME_LEN];
		if (cid <= 1000)
			Lastname(cid - 1, c_last);
		else
			Lastname(NURand(255,0,999,wid-1), c_last);
		row->set_value(C_LAST, c_last);
#if !CHBENCH_SMALL
		char tmp[3] = "OE";
		row->set_value(C_MIDDLE, tmp);
		char c_first[FIRSTNAME_LEN];
		MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first, wid-1);
		row->set_value(C_FIRST, c_first);
		char street[20];
		MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_1, street);
		MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_STREET_2, street);
		MakeAlphaString(10, 20, street, wid-1);
		row->set_value(C_CITY, street); 
		char state[2];
		MakeAlphaString(2, 2, state, wid-1); /* State */
		row->set_value(C_STATE, state);
		char zip[9];
		MakeNumberString(9, 9, zip, wid-1); /* Zip */
		row->set_value(C_ZIP, zip);
		char phone[16];
  		MakeNumberString(16, 16, phone, wid-1); /* Zip */
		row->set_value(C_PHONE, phone);
		row->set_value(C_SINCE, 0);
		row->set_value(C_CREDIT_LIM, 50000);
		row->set_value(C_DELIVERY_CNT, 0);
		char c_data[500];
		MakeAlphaString(300, 500, c_data, wid-1);
		row->set_value(C_DATA, c_data);
#endif
		if (RAND(10, wid-1) == 0) {
			char tmp[] = "GC";
			row->set_value(C_CREDIT, tmp);
		} else {
			char tmp[] = "BC";
			row->set_value(C_CREDIT, tmp);
		}
		row->set_value(C_DISCOUNT, (double)RAND(5000,wid-1) / 10000);
		row->set_value(C_BALANCE, -10.0);
		row->set_value(C_YTD_PAYMENT, 10.0);
		row->set_value(C_PAYMENT_CNT, 1);
		uint64_t key;
		key = custNPKey(c_last, did, wid);
		index_insert(i_customer_last, key, row, wh_to_part(wid));
		key = custKey(cid, did, wid);
		index_insert(i_customer_id, key, row, wh_to_part(wid));

#if CHBENCH_EVA_CUBIT
		key = distKey(did - 1, wid - 1);
		index_insert(i_customers, key, row, wh_to_part(wid));
#endif
	}
}

void chbench_wl::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
	row_t * row;
	uint64_t row_id;
	t_history->get_new_row(row, 0, row_id);
	row->set_primary_key(0);
	row->set_value(H_C_ID, c_id);
	row->set_value(H_C_D_ID, d_id);
	row->set_value(H_D_ID, d_id);
	row->set_value(H_C_W_ID, w_id);
	row->set_value(H_W_ID, w_id);
	row->set_value(H_DATE, 0);
	row->set_value(H_AMOUNT, 10.0);
#if !CHBENCH_SMALL
	char h_data[24];
	MakeAlphaString(12, 24, h_data, w_id-1);
	row->set_value(H_DATA, h_data);
#endif

}

void chbench_wl::init_tab_order(uint64_t did, uint64_t wid) {

	uint64_t perm[g_cust_per_dist]; 
	init_permutation(perm, wid); /* initialize permutation of customer numbers */
	for (UInt32 oid = 1; oid <= g_cust_per_dist; oid++) {
		row_t * row;
		uint64_t row_id;
		t_order->get_new_row_seq(row, wh_to_part(wid), row_id);
		row->set_primary_key(oid);
		uint64_t o_ol_cnt = 1;
		uint64_t cid = perm[oid - 1]; //get_permutation();
		row->set_value(O_ID, oid);
		row->set_value(O_C_ID, cid);
		row->set_value(O_D_ID, did);
		row->set_value(O_W_ID, wid);

		Date l(1983, 1, 1);
		Date r(2023, 12, 31);
		Date o_entry_date = r.URandDate(l, rng[wid - 1]);
		uint64_t o_entry = o_entry_date.DateToUint64();
		assert(o_entry_date >= l && o_entry_date <= r);
		row->set_value(O_ENTRY_D, o_entry);
		if (oid < 2101)
			row->set_value(O_CARRIER_ID, URand(1, 10, wid-1));
		else 
			row->set_value(O_CARRIER_ID, 0);
		o_ol_cnt = URand(5, 15, wid-1);
		row->set_value(O_OL_CNT, o_ol_cnt);
		row->set_value(O_ALL_LOCAL, 1);
		
		// ORDER-LINE	
#if !CHBENCH_SMALL

		for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
			t_orderline->get_new_row_seq(row, 0, row_id);
			row->set_value(OL_O_ID, oid);
			row->set_value(OL_D_ID, did);
			row->set_value(OL_W_ID, wid);
			row->set_value(OL_NUMBER, ol);
			row->set_value(OL_I_ID, URand(1, 100000, wid-1));
			row->set_value(OL_SUPPLY_W_ID, wid);
			uint64_t key; // create key for chbench_q6 index

			// we modified ‘delivery_date’ and 'orderline_quantity', 
			// because the original data for TPCC can't meet the requirement of CH_BENCHMARK's Q6

			int64_t ol_quantity = (int64_t)URand(1, 25000, wid-1);
			if (oid < 2101) {
				row->set_value(OL_DELIVERY_D, o_entry);
				// row->set_value(OL_AMOUNT, (double)0);
				row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
				key = chbenchQ6Key(ol_quantity, o_entry);
				if(ol == 1 && o_entry >= 20070102) {
					ATOM_ADD(cnt_q1_number1, 1);
					ATOM_ADD(q1_number1_quantity, ol_quantity);
				}
			} else {
				row->set_value(OL_DELIVERY_D, (uint64_t)0);
				row->set_value(OL_AMOUNT, (double)URand(1, 999999, wid-1)/100);
				key = chbenchQ6Key(ol_quantity, 0);
			}
			row->set_value(OL_QUANTITY, ol_quantity);
			char ol_dist_info[24];
			MakeAlphaString(24, 24, ol_dist_info, wid-1);
			row->set_value(OL_DIST_INFO, ol_dist_info);

			// index insert
			if(oid < 2101) {
				uint64_t primary_key = orderlineKey(wid, did, oid) * 100;
				switch(CHBENCH_QUERY_METHOD) {
					case CHBenchQueryMethod::SCAN_METHOD :
						break;
					case CHBenchQueryMethod::BITMAP_METHOD :
						break;
					case CHBenchQueryMethod::BITMAP_PARA_METHOD :
						break;
					case CHBenchQueryMethod::BTREE_METHOD :
						tree_insert((INDEX*)i_orderline_d, (INDEX*)i_orderline, o_entry, key, row, 0);
						break;
					case CHBenchQueryMethod::BWTREE_METHOD :
						i_Q6_bwtree->AssignGCID(wid - 1);
						i_Q1_bwtree->AssignGCID(wid - 1);
						tree_insert((INDEX*)i_Q1_bwtree, (INDEX*)i_Q6_bwtree, o_entry, key, row, 0);
						i_Q6_bwtree->UnregisterThread(wid - 1);
						i_Q1_bwtree->UnregisterThread(wid - 1);
						break;
					case CHBenchQueryMethod::ART_METHOD :
						primary_key += ol;
						tree_insert((INDEX*)i_Q1_art, (INDEX*)i_Q6_art, o_entry, key, row, primary_key);
						break;
					case CHBenchQueryMethod::ALL_METHOD :
						primary_key += ol;
						index_insert_with_primary_key((INDEX *)i_Q6_art, key, primary_key, row, 0);
						index_insert((INDEX*)i_orderline, key, row, 0);
						i_Q6_bwtree->AssignGCID(wid - 1);
						index_insert((INDEX*)i_Q6_bwtree, key, row, 0);
						i_Q6_bwtree->UnregisterThread(wid - 1);
						break;
				}
			}
				

			//for debug
			if((bitmap_deliverydate_bin(oid < 2101?o_entry:(uint64_t)0) == 1) && bitmap_quantity_bin(ol_quantity) == 1)
				ATOM_ADD(cnt_q6, 1);



			// bitmap insert
			// if (Mode != "cache")
			if (Mode == NULL || (Mode && strcmp(Mode, "build") == 0))
			{
				cubit::Cubit *bitmap = NULL;
				if(query_number == CHBenchQuery::CHBenchQ6) {
					bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q6_deliverydate);

					bitmap->__init_append(0, row_id, bitmap_deliverydate_bin(oid < 2101?o_entry:(uint64_t)0));

					bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q6_quantity);
					// bitmap->__init_append(0, row_id2, quantity-1);
					bitmap->__init_append(0, row_id, bitmap_quantity_bin(ol_quantity));	
				}
				else {
					bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q1_deliverydate);

					bitmap->__init_append(0, row_id, oid < 2101 ?(o_entry >= CHBENCH_Q1_MIN_DELIVERY_DATE ):(uint64_t)0);

					bitmap = dynamic_cast<cubit::Cubit *>(bitmap_q1_ol_number);

					bitmap->__init_append(0, row_id, static_cast<int>(ol-1));
				}
			}
		}
#endif
		// NEW ORDER
		if (oid > 2100) {
			t_neworder->get_new_row(row, 0, row_id);
			row->set_value(NO_O_ID, oid);
			row->set_value(NO_D_ID, did);
			row->set_value(NO_W_ID, wid);
		}
	}
}

void chbench_wl::init_tab_region()
{
	const char *name[] = {"Africa", "Asia", "Europe", "North America", "South America"};
	for (UInt32 i = 1; i <= 5; i++)
	{
		row_t *row;
		uint64_t row_id;
		t_region->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(R_REGIONKEY, i);
		// Create a char array to hold the name
		char region_name[25]; // Assuming R_NAME has a max size of 25 characters
		strncpy(region_name, name[i - 1], sizeof(region_name));
		region_name[sizeof(region_name) - 1] = '\0'; // Ensure null-termination
		row->set_value(R_NAME, region_name);
		char comment[152];
		MakeAlphaString(52, 152, comment, 0);
		row->set_value(R_COMMENT, comment);

		index_insert(i_region, i, row, 0);
	}
}

void chbench_wl::init_tab_nation()
{
	for (UInt32 i = 1; i <= 25; i++)
	{
		row_t *row;
		uint64_t row_id;
		t_nation->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(N_NATIONKEY, i);
		char name[25];
		MakeAlphaString(15, 25, name, 0);
		row->set_value(N_NATIONKEY, name);
		row->set_value(N_REGIONKEY, URand(1, 5, 0));
		char comment[152];
		MakeAlphaString(52, 152, comment, 0);
		row->set_value(N_COMMENT, comment);

		index_insert(i_nation, i, row, 0);
	}
}

void chbench_wl::init_tab_supplier()
{
	for (UInt32 i = 1; i <= SF * 10000; i++)
	{
		row_t *row;
		uint64_t row_id;
		t_supplier->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(S_SUPPKEY, i);
		char name[25];
		MakeAlphaString(15, 25, name, 0);
		row->set_value(S_NAME, name);
		row->set_value(S_NATIONKEY, URand(1, 25, 0));
		char address[40];
		MakeAlphaString(20, 40, address, 0);
		row->set_value(S_ADDRESS, address);
		char phone[15];
		MakeAlphaString(15, 15, phone, 0);
		row->set_value(S_PHONE, phone);
		row->set_value(S_ACCTBAL, (double)URand(1, 999999, 0) / 100);
		char comment[101];
		MakeAlphaString(51, 101, comment, 0);
		row->set_value(S_COMMENT, comment);

		index_insert(i_supplier, i, row, 0);
	}
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void 
chbench_wl::init_permutation(uint64_t * perm_c_id, uint64_t wid) {
	uint32_t i;
	// Init with consecutive values
	for(i = 0; i < g_cust_per_dist; i++) 
		perm_c_id[i] = i+1;

	// shuffle
	for(i=0; i < g_cust_per_dist-1; i++) {
		uint64_t j = URand(i+1, g_cust_per_dist-1, wid-1);
		uint64_t tmp = perm_c_id[i];
		perm_c_id[i] = perm_c_id[j];
		perm_c_id[j] = tmp;
	}
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

void * chbench_wl::threadInitWarehouse(void * This) {
	chbench_wl * wl = (chbench_wl *) This;
	uint32_t tid = ATOM_FETCH_ADD(wl->next_tid, 1);
	uint32_t wid = tid + 1;
	tpc_buffer[tid] = (drand48_data *) _mm_malloc(sizeof(drand48_data), 64);
	rng[tid].seed(tid);
	assert((uint64_t)tid < g_num_wh);
	srand48_r(wid, tpc_buffer[tid]);
	// cout << *tpc_buffer[tid]->__x << "_" << wid << endl;
	if (tid == 0) 
	{
		wl->init_tab_item();
		wl->init_tab_region();
		wl->init_tab_nation();
		wl->init_tab_supplier();
	}
	wl->init_tab_wh( wid );
	wl->init_tab_dist( wid );
	wl->init_tab_stock( wid );
	for (uint64_t did = 1; did <= DIST_PER_WARE; did++) {
		wl->init_tab_cust(did, wid);
		wl->init_tab_order(did, wid);
		for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) 
			wl->init_tab_hist(cid, did, wid);
	}
	return NULL;
}


RC chbench_wl::init_bitmap() 
{
	// auto start = std::chrono::high_resolution_clock::now();
	// auto end = std::chrono::high_resolution_clock::now();
	// long  long  time = (long  long)0;	

	uint64_t n_rows = 0UL;
	if (Mode && strcmp(Mode, "cache") == 0) {
		fstream done;
		string path;
		if(query_number == CHBenchQuery::CHBenchQ6)
			path = "bm_chbench_n" + to_string(g_num_wh) + "_q6_done";
		else path = "bm_chbench_n" + to_string(g_num_wh) + "_q1_done";
		done.open(path, ios::in);
		if (done.is_open()) {
			done >> n_rows;
			cout << "[cache] Number of rows: " << n_rows << " . Fetched from bitmap cached file: " << path << endl;
			done.close();
		}
		else {
			cout << "[cache] Failed to open the specified bitmap cached file: " << path << endl;
			exit(-1);
		}

	}
	db_number_of_rows = n_rows;
if(query_number == CHBenchQuery::CHBenchQ6) {
/********************* bitmap_deliverydate_q6 ******************************/
	{
	Table_config *config_deliverydate = new Table_config{};
	config_deliverydate->n_workers = g_thread_cnt;
	config_deliverydate->DATA_PATH = "";
	if (Mode && strcmp(Mode, "cache") == 0) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q6_deliverydate");
		config_deliverydate->INDEX_PATH = temp;
	} else
		config_deliverydate->INDEX_PATH = "";
	config_deliverydate->n_rows = n_rows; 
	config_deliverydate->g_cardinality = 3; // {<1999, [1999,2020), >=2020}
	enable_fence_pointer = config_deliverydate->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_deliverydate->approach = {"cubit-lk"};
	config_deliverydate->nThreads_for_getval = 4;
	config_deliverydate->show_memory = true;
	config_deliverydate->on_disk = false;
	config_deliverydate->showEB = false;
	config_deliverydate->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_deliverydate->n_queries = MAX_TXN_PER_PART * 5;
	config_deliverydate->n_udis = MAX_TXN_PER_PART * 0.1;
	config_deliverydate->verbose = false;
	config_deliverydate->time_out = 100;
	config_deliverydate->autoCommit = false;
	config_deliverydate->n_merge_threshold = 16;
	config_deliverydate->db_control = true;

	config_deliverydate->segmented_btv = false;
	// if(CHBENCH_QUERY_METHOD == BITMAP_PARA_METHOD) {
	// 	config_deliverydate->segmented_btv = true;
	// }
	config_deliverydate->encoded_word_len = 31;
	config_deliverydate->rows_per_seg = 100000;
	config_deliverydate->enable_parallel_cnt = false;
	
	// start = std::chrono::high_resolution_clock::now();
	if (config_deliverydate->approach == "ub") {
		bitmap_q6_deliverydate = new ub::Table(config_deliverydate);
	} else if (config_deliverydate->approach == "cubit-lk") {
		bitmap_q6_deliverydate = new cubit_lk::CubitLK(config_deliverydate);
	} else if (config_deliverydate->approach == "cubit-lf" || config_deliverydate->approach =="cubit") {
		bitmap_q6_deliverydate = new cubit_lf::CubitLF(config_deliverydate);
	} else if (config_deliverydate->approach == "ucb") {
		bitmap_q6_deliverydate = new ucb::Table(config_deliverydate);
	} else if (config_deliverydate->approach == "naive") {
		bitmap_q6_deliverydate = new naive::Table(config_deliverydate);
	}
	else {
		cerr << "Unknown approach." << endl;
		exit(-1);
	}
	// end = std::chrono::high_resolution_clock::now();
	// time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	cout << "[CUBIT]: Bitmap bitmap_deliverydate initialized successfully. "
			<< "[Cardinality:" << config_deliverydate->g_cardinality
			<< "] [Method:" << config_deliverydate->approach << "]" << endl;
	}



/********************* bitmap_quantity_q6 ******************************/
	{
	Table_config *config_quantity = new Table_config{};
	config_quantity->n_workers = g_thread_cnt;
	config_quantity->DATA_PATH = "";
	if (Mode && strcmp(Mode, "cache") == 0) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q6_quantity");
		config_quantity->INDEX_PATH = temp;
	} else
		config_quantity->INDEX_PATH = "";
	config_quantity->n_rows = n_rows;  
	// config_quantity->g_cardinality = 50; // [0, 49]
	config_quantity->g_cardinality = 3; // [<1], [1,1000], [>1000]
	enable_fence_pointer = config_quantity->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_quantity->approach = {"cubit-lk"};
	config_quantity->nThreads_for_getval = 4;
	config_quantity->show_memory = true;
	config_quantity->on_disk = false;
	config_quantity->showEB = false;
	config_quantity->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_quantity->n_queries = MAX_TXN_PER_PART * 5;
	config_quantity->n_udis = MAX_TXN_PER_PART * 0.1;
	config_quantity->verbose = false;
	config_quantity->time_out = 100;
	config_quantity->autoCommit = false;
	config_quantity->n_merge_threshold = 16;
	config_quantity->db_control = true;

	config_quantity->segmented_btv = false;
	// if(CHBENCH_QUERY_METHOD == BITMAP_PARA_METHOD) {
	// 	config_quantity->segmented_btv = true;
	// }
	config_quantity->encoded_word_len = 31;
	config_quantity->rows_per_seg = 100000;
	config_quantity->enable_parallel_cnt = false;
	
	// start = std::chrono::high_resolution_clock::now();
	if (config_quantity->approach == "ub") {
		bitmap_q6_quantity = new ub::Table(config_quantity);
	} else if (config_quantity->approach == "cubit-lk") {
		bitmap_q6_quantity = new cubit_lk::CubitLK(config_quantity);
	} else if (config_quantity->approach == "cubit-lf" || config_quantity->approach =="cubit") {
		bitmap_q6_quantity = new cubit_lf::CubitLF(config_quantity);
	} else if (config_quantity->approach == "ucb") {
		bitmap_q6_quantity = new ucb::Table(config_quantity);
	} else if (config_quantity->approach == "naive") {
		bitmap_q6_quantity = new naive::Table(config_quantity);
	}
	else {
		cerr << "Unknown approach." << endl;
		exit(-1);
	}
	// end = std::chrono::high_resolution_clock::now();
	// time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();	

	cout << "[CUBIT]: Bitmap bitmap_quantity initialized successfully. "
			<< "[Cardinality:" << config_quantity->g_cardinality
			<< "] [Method:" << config_quantity->approach << "]" << endl;
	}

}

if(query_number == CHBenchQuery::CHBenchQ1) {
	/********************* bitmap_q1_deliverydate ******************************/
	{
	Table_config *config_q1 = new Table_config{};
	config_q1->n_workers = g_thread_cnt;
	config_q1->DATA_PATH = "";
	if (Mode && strcmp(Mode, "cache") == 0) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q1_deliverydate");
		config_q1->INDEX_PATH = temp;
	} else
		config_q1->INDEX_PATH = "";
	config_q1->n_rows = n_rows;  
	config_q1->g_cardinality = 2; // [<=2007], [>2007]
	enable_fence_pointer = config_q1->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_q1->approach = {"cubit-lk"};
	config_q1->nThreads_for_getval = 4;
	config_q1->show_memory = true;
	config_q1->on_disk = false;
	config_q1->showEB = false;
	config_q1->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_q1->n_queries = MAX_TXN_PER_PART * 5;
	config_q1->n_udis = MAX_TXN_PER_PART * 0.1;
	config_q1->verbose = false;
	config_q1->time_out = 100;
	config_q1->autoCommit = false;
	config_q1->n_merge_threshold = 16;
	config_q1->db_control = true;

	config_q1->segmented_btv = false;
	config_q1->encoded_word_len = 31;
	config_q1->rows_per_seg = 100000;
	config_q1->enable_parallel_cnt = false;
	
	// start = std::chrono::high_resolution_clock::now();
	if (config_q1->approach == "ub") {
		bitmap_q1_deliverydate = new ub::Table(config_q1);
	} else if (config_q1->approach == "cubit-lk") {
		bitmap_q1_deliverydate = new cubit_lk::CubitLK(config_q1);
	} else if (config_q1->approach == "cubit-lf" || config_q1->approach =="cubit") {
		bitmap_q1_deliverydate = new cubit_lf::CubitLF(config_q1);
	} else if (config_q1->approach == "ucb") {
		bitmap_q1_deliverydate = new ucb::Table(config_q1);
	} else if (config_q1->approach == "naive") {
		bitmap_q1_deliverydate = new naive::Table(config_q1);
	}
	else {
		cerr << "Unknown approach." << endl;
		exit(-1);
	}
	// end = std::chrono::high_resolution_clock::now();
	// time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();	

	cout << "[CUBIT]: Bitmap bitmap_q1_deliverydate initialized successfully. "
			<< "[Cardinality:" << config_q1->g_cardinality
			<< "] [Method:" << config_q1->approach << "]" << endl;
	}


	/********************* bitmap_q1_ol_number ******************************/
	{
	Table_config *config_ol_number = new Table_config{};
	config_ol_number->n_workers = g_thread_cnt;
	config_ol_number->DATA_PATH = "";
	if (Mode && strcmp(Mode, "cache") == 0) {
		string temp = "bm_chbench_n";
		temp.append(to_string(g_num_wh));
		temp.append("_q1_ol_number");
		config_ol_number->INDEX_PATH = temp;
	} else
		config_ol_number->INDEX_PATH = "";
	config_ol_number->n_rows = n_rows;  
	config_ol_number->g_cardinality = 15; // 0-15
	enable_fence_pointer = config_ol_number->enable_fence_pointer = true;
	INDEX_WORDS = 10000;  // Fence length 
	config_ol_number->approach = {"cubit-lk"};
	config_ol_number->nThreads_for_getval = 4;
	config_ol_number->show_memory = true;
	config_ol_number->on_disk = false;
	config_ol_number->showEB = false;
	config_ol_number->decode = false;

	// DBx1000 doesn't use the following parameters;
	// they are used by nicolas.
	config_ol_number->n_queries = MAX_TXN_PER_PART * 5;
	config_ol_number->n_udis = MAX_TXN_PER_PART * 0.1;
	config_ol_number->verbose = false;
	config_ol_number->time_out = 100;
	config_ol_number->autoCommit = false;
	config_ol_number->n_merge_threshold = 16;
	config_ol_number->db_control = true;

	config_ol_number->segmented_btv = false;
	config_ol_number->encoded_word_len = 31;
	config_ol_number->rows_per_seg = 100000;
	config_ol_number->enable_parallel_cnt = false;
	
	// start = std::chrono::high_resolution_clock::now();
	if (config_ol_number->approach == "ub") {
		bitmap_q1_ol_number = new ub::Table(config_ol_number);
	} else if (config_ol_number->approach == "cubit-lk") {
		bitmap_q1_ol_number = new cubit_lk::CubitLK(config_ol_number);
	} else if (config_ol_number->approach == "cubit-lf" || config_ol_number->approach =="cubit") {
		bitmap_q1_ol_number = new cubit_lf::CubitLF(config_ol_number);
	} else if (config_ol_number->approach == "ucb") {
		bitmap_q1_ol_number = new ucb::Table(config_ol_number);
	} else if (config_ol_number->approach == "naive") {
		bitmap_q1_ol_number = new naive::Table(config_ol_number);
	}
	else {
		cerr << "Unknown approach." << endl;
		exit(-1);
	}
	// end = std::chrono::high_resolution_clock::now();
	// time += std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();	

	cout << "[CUBIT]: Bitmap bitmap_q1_ol_number initialized successfully. "
			<< "[Cardinality:" << config_ol_number->g_cardinality
			<< "] [Method:" << config_ol_number->approach << "]" << endl;
	}

}
	// cout << "INDEX bitmap build time:" << time << endl;

	return RCOK;
}

void chbench_wl::tree_insert(INDEX *tree_q1, INDEX *tree_q6, uint64_t key_q1, uint64_t key_q6, row_t *row, uint64_t primary_key) {
	if(query_number == CHBenchQuery::CHBenchQ6) { 
		switch(CHBENCH_QUERY_METHOD) {
			case CHBenchQueryMethod::ART_METHOD :
				index_insert_with_primary_key((INDEX *)tree_q6, key_q6, primary_key, row, 0);
				break;
			case CHBenchQueryMethod::BWTREE_METHOD :
				;
			case CHBenchQueryMethod::BTREE_METHOD :
				index_insert((INDEX*)tree_q6, key_q6, row, 0);;
				break;
			case CHBenchQueryMethod::ALL_METHOD :
				index_insert_with_primary_key((INDEX *)tree_q6, key_q6, primary_key, row, 0);
				index_insert((INDEX*)tree_q6, key_q6, row, 0);;
				break;
			default :
				assert(0);
		}
	}
	else if(query_number == CHBenchQuery::CHBenchQ1) {
		switch(CHBENCH_QUERY_METHOD) {
			case CHBenchQueryMethod::ART_METHOD :
				index_insert_with_primary_key((INDEX *)tree_q1, key_q1, primary_key, row, 0);
				break;
			case CHBenchQueryMethod::BWTREE_METHOD :
				;
			case CHBenchQueryMethod::BTREE_METHOD :
				index_insert((INDEX*)tree_q1, key_q1, row, 0);;
				break;
			default :
				assert(0);
		}
	}
	else {
		assert(0);
	} 
}
