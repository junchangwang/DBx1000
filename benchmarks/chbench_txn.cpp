#include "chbench.h"
#include "manager.h"
#include "chbench_query.h"
#include "tpc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "index_bwtree.h"
#include "index_art.h"
#include "chbench_const.h"
#include "output_log.h"
#include "Date.h"
#include "perf.h"
using namespace rabit;
#define WAIT_FOR_PERF_U (1000 * 50)

extern bool perf_enabled;

extern 	CHBenchQuery query_number;
int new_order_numbers;
int ol_num_numbers[15];

chbench_q1_data::chbench_q1_data(int size) {
	d_size = size;
	cnt = (int*) _mm_malloc(sizeof(int) * d_size, 64);
	memset(cnt, 0, d_size * sizeof(int));
	sum_amount = (double*) _mm_malloc(sizeof(double) * d_size, 64);
	
	avg_amount = (double*) _mm_malloc(sizeof(double) * d_size, 64);

	avg_quantity = (double*) _mm_malloc(sizeof(double) * d_size, 64);

	sum_quantity = (uint64_t*) _mm_malloc(sizeof(uint64_t) * d_size, 64);

	for(int i = 0; i < d_size; i++) {
		sum_amount[i] = 0;
		avg_amount[i] = 0;
		avg_quantity[i] = 0;
		cnt[i] = 0;
		sum_quantity[i] = 0;
	}

}

chbench_q1_data::chbench_q1_data(const chbench_q1_data& obj) {
	d_size = obj.d_size;
	cnt = (int*) _mm_malloc(sizeof(int) * d_size, 64);
	memset(cnt, 0, d_size * sizeof(int));
	sum_amount = (double*) _mm_malloc(sizeof(double) * d_size, 64);

	avg_amount = (double*) _mm_malloc(sizeof(double) * d_size, 64);

	avg_quantity = (double*) _mm_malloc(sizeof(double) * d_size, 64);

	sum_quantity = (uint64_t*) _mm_malloc(sizeof(uint64_t) * d_size, 64);

	for(int i = 0; i < d_size; i++) {
		sum_amount[i] = 0;
		avg_amount[i] = 0;
		avg_quantity[i] = 0;
		cnt[i] = 0;
		sum_quantity[i] = 0;
	}
}

chbench_q1_data::~chbench_q1_data() {
	_mm_free(cnt);
	_mm_free(sum_amount);
	_mm_free(sum_quantity);
	_mm_free(avg_amount);
	_mm_free(avg_quantity);
}


void chbench_q1_data::operator+=(const chbench_q1_data& tmp) {
	for(int i = 0; i < d_size; i++) {
		sum_amount[i] += tmp.sum_amount[i];
		cnt[i] += tmp.cnt[i];
		sum_quantity[i] += tmp.sum_quantity[i];
	}
	return;
}

void chbench_q1_data::operator=(const chbench_q1_data& tmp) {
	for(int i = 0; i < d_size; i++) {
		sum_amount[i] = tmp.sum_amount[i];
		cnt[i] = tmp.cnt[i];
		sum_quantity[i] = tmp.sum_quantity[i];
	}
	return;
}

void chbench_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (chbench_wl *) h_wl;
}

RC chbench_txn_man::run_txn(int tid, base_query * query) {
	chbench_query * m_query = (chbench_query *) query;
#if CHBENCH_EVA_RABIT && (CHBENCH_EVA_RABIT == CHBENCH)
	return evaluate_index(m_query);
#else
	rabit::Rabit *bitmap = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_deliverydate);
	switch (m_query->type) {
		case CHBENCH_PAYMENT :
			return run_payment(tid, m_query); break;
		case CHBENCH_NEW_ORDER :
			return run_new_order(tid, m_query); break;
/*		case TPCC_ORDER_STATUS :
			return run_order_status(m_query); break;
		case TPCC_DELIVERY :
			return run_delivery(m_query); break;
		case TPCC_STOCK_LEVEL :
			return run_stock_level(m_query); break;*/
		case CHBENCH_Q6_SCAN :
			return run_Q6_scan(tid, m_query); break;
		case CHBENCH_Q6_BTREE :
			return run_Q6_btree(tid, m_query); break;
		case CHBENCH_Q6_BITMAP :
			if(CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD) {
/*				bitmap = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_quantity);
				assert(bitmap->config->segmented_btv);*/
				return run_Q6_bitmap_parallel(tid, m_query);
			}
			return run_Q6_bitmap(tid, m_query); break;
		case CHBENCH_Q6_BITMAP_PARALLEL :
			return run_Q6_bitmap_parallel(tid, m_query);
		case CHBENCH_Q6_BWTREE :
			return run_Q6_bwtree(tid, m_query); break;
		case CHBENCH_Q6_ART :
			return run_Q6_art(tid, m_query); break;
		case CHBENCH_Q1_SCAN :
			return run_Q1_scan(tid, m_query); break;
		case CHBENCH_Q1_BTREE :
			return run_Q1_btree(tid, m_query); break;
		case CHBENCH_Q1_BITMAP :
			return run_Q1_bitmap(tid, m_query); break;
		case CHBENCH_Q1_BITMAP_PARALLEL :
			return run_Q1_bitmap_parallel_fetch(tid, m_query); break;
		case CHBENCH_Q1_BWTREE :
			return run_Q1_bwtree(tid, m_query); break;
		case CHBENCH_Q1_ART :
			return run_Q1_art(tid, m_query); break;
		case CHBENCH_VERIFY_TABLE :
			return validate_table(tid, m_query); break;
		default:
			assert(false);
	}
#endif
}

RC chbench_txn_man::evaluate_index(chbench_query * query) 
{
	RC rc = RCOK;
	itemid_t * item;

	// m_lock is used to sequentially debug and execute evaluate_index()
	// to evaluate RABIT. It can be removed in real code.
	static std::mutex m_lock;
	std::lock_guard<std::mutex> gard(m_lock);

	// Assume the query arguments specified by the programmer.
	uint64_t c_w_id = 0;
	uint64_t c_d_id = 4;
	std::string t_last = "OUGHTESEPRI";

	/*==========================================================+
		To evaluate RABIT, we perform the following query:

		EXEC SQL SELECT c_id INTO :c_ids
		FROM customer
		WHERE c_d_id=:c_d_id AND c_w_id=:c_w_id;

		which is a notable sub query of use cases such as the following

		EXEC SQL SELECT count(c_id) INTO :namecnt
		FROM customer
		WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
	+==========================================================*/

	int cnt = 0;
	int tmp = 0;
	uint64_t key = distKey(c_d_id, c_w_id);
	INDEX * index = _wl->i_customers;
	item = index_read(index, key, wh_to_part(query->c_w_id));
	assert(item != NULL);

	auto start = std::chrono::high_resolution_clock::now();
	for (tmp = 0; item; tmp++) {
		row_t *r_cust = ((row_t *)item->location);
		row_t *r_cust_local = get_row(r_cust, RD);
		if (r_cust_local == NULL) {
			return RCOK;
		}

		// For a fair comparasion, we don't touch the memory of the tuple
		// to avoid cache misses.

		// char c_last[LASTNAME_LEN];
		// char *tmp_str = r_cust_local->get_value(C_LAST);
		// memcpy(c_last, tmp_str, LASTNAME_LEN-1);
		// c_last[LASTNAME_LEN-1] = '\0';

		// if (!strcmp(t_last.c_str(), c_last)) {
		// 	cnt ++;
		// }

		item = item->next;
	}
	auto end = std::chrono::high_resolution_clock::now();
	long  long time_elapsed_ms = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
	cout << "HashTable: Loop times: " << tmp << ". Found string times: " << cnt << ". Microseconds: " << time_elapsed_ms << endl;
	cout << "Memory concumption (Bytes): " << index->index_size() << endl;


	exit(rc);
	return rc;
}


RC chbench_txn_man::run_payment(int tid, chbench_query * query) {
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;

	uint64_t w_id = query->w_id;
	uint64_t c_w_id = query->c_w_id;
	/*====================================================+
		EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/

	// TODO for variable length variable (string). Should store the size of 
	// the variable.
	key = query->w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	row_t * r_wh_local;
	if (g_wh_update)
		r_wh_local = get_row(r_wh, WR);
	else 
		r_wh_local = get_row(r_wh, RD);

	if (r_wh_local == NULL) {
		return finish(Abort);
	}
	double w_ytd;
	
	r_wh_local->get_value(W_YTD, w_ytd);
	if (g_wh_update) {
		r_wh_local->set_value(W_YTD, w_ytd + query->h_amount);
	}
	char w_name[11];
	char * tmp_str = r_wh_local->get_value(W_NAME);
	memcpy(w_name, tmp_str, 10);
	w_name[10] = '\0';
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	key = distKey(query->d_id, query->d_w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	row_t * r_dist_local = get_row(r_dist, WR);
	if (r_dist_local == NULL) {
		return finish(Abort);
	}

	double d_ytd;
	r_dist_local->get_value(D_YTD, d_ytd);
	r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
	char d_name[11];
	tmp_str = r_dist_local->get_value(D_NAME);
	memcpy(d_name, tmp_str, 10);
	d_name[10] = '\0';

	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/

	row_t * r_cust;
	if (query->by_last_name) { 
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/

		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
		
		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	}
	else { // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}

	/*======================================================================+
		EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
	+======================================================================*/
	row_t * r_cust_local = get_row(r_cust, WR);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	double c_ytd_payment;
	double c_payment_cnt;

	r_cust_local->get_value(C_BALANCE, c_balance);
	r_cust_local->set_value(C_BALANCE, c_balance - query->h_amount);
	r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
	r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + query->h_amount);
	r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
	r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

	char * c_credit = r_cust_local->get_value(C_CREDIT);

	if ( strstr(c_credit, "BC") ) {
	
		/*=====================================================+
			EXEC SQL SELECT c_data
			INTO :c_data
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+=====================================================*/
		// char c_new_data[501];
		// sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f",
		// 	c_id, c_d_id, c_w_id, d_id, w_id, query->h_amount);
		// char * c_data = r_cust->get_value("C_DATA");
		// strncat(c_new_data, c_data, 500 - strlen(c_new_data));
		// r_cust->set_value("C_DATA", c_new_data);
			
	}
	
	char h_data[25];
	strncpy(h_data, w_name, 10);
	int length = strlen(h_data);
	if (length > 10) length = 10;
	strcpy(&h_data[length], "    ");
	strncpy(&h_data[length + 4], d_name, 10);
	h_data[length+14] = '\0';
	/*=============================================================================+
		EXEC SQL INSERT INTO
		history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
		VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
		+=============================================================================*/
//	row_t * r_hist;
//	uint64_t row_id;
//	_wl->t_history->get_new_row(r_hist, 0, row_id);
//	r_hist->set_value(H_C_ID, c_id);
//	r_hist->set_value(H_C_D_ID, c_d_id);
//	r_hist->set_value(H_C_W_ID, c_w_id);
//	r_hist->set_value(H_D_ID, d_id);
//	r_hist->set_value(H_W_ID, w_id);
//	int64_t date = 2013;		
//	r_hist->set_value(H_DATE, date);
//	r_hist->set_value(H_AMOUNT, h_amount);
#if !CHBENCH_SMALL
//	r_hist->set_value(H_DATA, h_data);
#endif
//	insert_row(r_hist, _wl->t_history);

	if( CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_METHOD || CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD ) {
		lock_guard<shared_mutex> guard(bitmap_mutex);
		assert( rc == RCOK );
		return finish(rc, 1);
	}

	assert( rc == RCOK );
	return finish(rc);
}

RC chbench_txn_man::run_new_order(int tid, chbench_query * query) {
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;
	INDEX * index;
	
	bool remote = query->remote;
	uint64_t w_id = query->w_id;
	uint64_t d_id = query->d_id;
	uint64_t c_id = query->c_id;
	uint64_t ol_cnt = query->ol_cnt;
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	row_t * r_wh_local = get_row(r_wh, RD);
	if (r_wh_local == NULL) {
		return finish(Abort);
	}


	double w_tax;
	r_wh_local->get_value(W_TAX, w_tax); 
	key = custKey(c_id, d_id, w_id);
	index = _wl->i_customer_id;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;
	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort); 
	}
	uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	r_cust_local->get_value(C_DISCOUNT, c_discount);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
 	
	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	row_t * r_dist_local = get_row(r_dist, WR);
	if (r_dist_local == NULL) {
		return finish(Abort);
	}
	//double d_tax;
	int64_t o_id;
	//d_tax = *(double *) r_dist_local->get_value(D_TAX);
	// o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
	r_dist_local->get_value(D_NEXT_O_ID, o_id);
	o_id ++;
	r_dist_local->set_value(D_NEXT_O_ID, o_id);

	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
//	row_t * r_order;
//	uint64_t row_id;
//	_wl->t_order->get_new_row(r_order, 0, row_id);
//	r_order->set_value(O_ID, o_id);
//	r_order->set_value(O_C_ID, c_id);
//	r_order->set_value(O_D_ID, d_id);
//	r_order->set_value(O_W_ID, w_id);
//	r_order->set_value(O_ENTRY_D, o_entry_d);
//	r_order->set_value(O_OL_CNT, ol_cnt);
//	int64_t all_local = (remote? 0 : 1);
//	r_order->set_value(O_ALL_LOCAL, all_local);
//	insert_row(r_order, _wl->t_order);
	/*=======================================================+
	EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
		VALUES (:o_id, :d_id, :w_id);
	+=======================================================*/
//	row_t * r_no;
//	_wl->t_neworder->get_new_row(r_no, 0, row_id);
//	r_no->set_value(NO_O_ID, o_id);
//	r_no->set_value(NO_D_ID, d_id);
//	r_no->set_value(NO_W_ID, w_id);
//	insert_row(r_no, _wl->t_neworder);
	rabit::Rabit *bitmap_d = nullptr;
	rabit::Rabit *bitmap_ol_num = nullptr;
	rabit::Rabit *bitmap_q = nullptr;
	uint64_t inserted_max_row_id;

	if( CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_METHOD || CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD ) {
		if( query_number == CHBenchQuery::CHBenchQ1 ) {
			bitmap_d = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q1_deliverydate);
			bitmap_ol_num = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q1_ol_number);
		}
		else if( query_number == CHBenchQuery::CHBenchQ6 ){
			bitmap_d = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_deliverydate);
			bitmap_q = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_quantity);
		}
		else {
			assert(0);
		}
	}

	// for recording inserted rows
	vector<uint64_t> inserted_rows_id;
	vector<uint64_t> inserted_rows_quantity;
	vector<row_t *> inserted_rows;

	for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {

		uint64_t ol_i_id = query->items[ol_number].ol_i_id;
		uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
		uint64_t ol_quantity = query->items[ol_number].ol_quantity;
		/*===========================================+
		EXEC SQL SELECT i_price, i_name , i_data
			INTO :i_price, :i_name, :i_data
			FROM item
			WHERE i_id = :ol_i_id;
		+===========================================*/
		key = ol_i_id;
		item = index_read(_wl->i_item, key, 0);
		assert(item != NULL);
		row_t * r_item = ((row_t *)item->location);

		row_t * r_item_local = get_row(r_item, RD);
		if (r_item_local == NULL) {
			return finish(Abort);
		}
		int64_t i_price;
		//char * i_name;
		//char * i_data;
		r_item_local->get_value(I_PRICE, i_price);
		//i_name = r_item_local->get_value(I_NAME);
		//i_data = r_item_local->get_value(I_DATA);

		/*===================================================================+
		EXEC SQL SELECT s_quantity, s_data,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data,
				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
			FROM stock
			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
			WHERE s_i_id = :ol_i_id
			AND s_w_id = :ol_supply_w_id;
		+===============================================*/

		uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
		INDEX * stock_index = _wl->i_stock;
		itemid_t * stock_item;
		index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
		assert(item != NULL);
		row_t * r_stock = ((row_t *)stock_item->location);
		row_t * r_stock_local = get_row(r_stock, WR);
		if (r_stock_local == NULL) {
			return finish(Abort);
		}
		
		// XXX s_dist_xx are not retrieved.
		UInt64 s_quantity;
		int64_t s_remote_cnt;
		s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !CHBENCH_SMALL
		int64_t s_ytd;
		int64_t s_order_cnt;
		//char * s_data = "test";
		r_stock_local->get_value(S_YTD, s_ytd);
		r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
		r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
		r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
		//s_data = r_stock_local->get_value(S_DATA);
#endif
		if (remote) {
			s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
			s_remote_cnt ++;
			r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
		}
		uint64_t quantity;
		if (s_quantity > ol_quantity + 10) {
			quantity = s_quantity - ol_quantity;
		} else {
			quantity = s_quantity - ol_quantity + 91;
		}
		r_stock_local->set_value(S_QUANTITY, &quantity);



		// need : change orderline

		/*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
		// XXX district info is not inserted.
		
		ol_number++;
		row_t * r_ol;
		uint64_t row_id;
		_wl->t_orderline->get_new_row_seq(r_ol, 0, row_id);
		inserted_max_row_id = row_id;
		r_ol->set_value(OL_O_ID, &o_id);
		r_ol->set_value(OL_D_ID, &d_id);
		r_ol->set_value(OL_W_ID, &w_id);
		r_ol->set_value(OL_NUMBER, &ol_number);
		r_ol->set_value(OL_I_ID, &ol_i_id);
#if !CHBENCH_SMALL
		int w_tax=1, d_tax=1;
		int64_t ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
		r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
		r_ol->set_value(OL_QUANTITY, &ol_quantity);
		r_ol->set_value(OL_AMOUNT, &ol_amount);
		r_ol->set_value(OL_DELIVERY_D, &query->o_entry_d);
#endif		

		ol_number--;
		insert_row(r_ol, _wl->t_orderline);
		if(CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_METHOD || CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD) {
			inserted_rows_id.push_back(row_id);
			if(query_number == CHBenchQuery::CHBenchQ1) {
			}
			else if (query_number == CHBenchQuery::CHBenchQ6) {
				inserted_rows_quantity.push_back(ol_quantity);
			}
			else {
				assert(0);
			}
		}

		if(CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BWTREE_METHOD || CHBENCH_QUERY_METHOD == CHBenchQueryMethod::ART_METHOD || \
			CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BTREE_METHOD) {
			inserted_rows.push_back(r_ol);
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ1) {
				;
			}
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ6) {
				inserted_rows_quantity.push_back(ol_quantity);
			}
		}
		
	}
	
	if( CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_METHOD || CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD ) {
		
		lock_guard<shared_mutex> guard(bitmap_mutex);

		// transaction begin
		bitmap_d->trans_begin(tid, get_ts());

		// append
		for(int i = 0; i < inserted_rows_id.size(); i++) {
			if( query_number == CHBenchQuery::CHBenchQ1 ) {
				bitmap_d->append(tid, 1, inserted_rows_id[i]);
			}
			else if( query_number == CHBenchQuery::CHBenchQ6 ) {
				bitmap_d->append(tid, bitmap_deliverydate_bin(query->o_entry_d), inserted_rows_id[i]);
			}
		}
		if(new_order_numbers % 5 == 4) {
			if( query_number == CHBenchQuery::CHBenchQ1 ) {
				bitmap_d->evaluate(tid, 1);
			}
			else if( query_number == CHBenchQuery::CHBenchQ6 ) {
				bitmap_d->evaluate(tid, bitmap_deliverydate_bin(query->o_entry_d));
			}
		}
		
		// transaction commit
		bitmap_d->trans_commit(tid, glob_manager->fetch_ts() + 1, inserted_max_row_id);

		// transaction begin
		if( query_number == CHBenchQuery::CHBenchQ1 ) {
			if( CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD ) {
				bitmap_ol_num->trans_begin(tid, get_ts());
			}
		}
		else if( query_number == CHBenchQuery::CHBenchQ6 ){
			bitmap_q->trans_begin(tid, get_ts());
		}
		else {
			assert(0);
		}

		// append
		for(int i = 0; i < inserted_rows_id.size(); i++) {
			if( query_number == CHBenchQuery::CHBenchQ1 ) {
				if( CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD ) {
					bitmap_ol_num->append(tid, i, inserted_rows_id[i]);
					ol_num_numbers[i]++;
				}
			}
			else if( query_number == CHBenchQuery::CHBenchQ6 ) {
				assert(i < inserted_rows_quantity.size());
				bitmap_q->append(tid, 1, inserted_rows_id[i]);
			}
			else {
				assert(0);
			}
		}

		//transaction commit
		if( query_number == CHBenchQuery::CHBenchQ1 ) {
			if( CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BITMAP_PARA_METHOD ) {
				int max_ol = 0;
				int ol_evaluate_val = -1;
				for(int i = 0; i < 15; i++) {
					if(ol_num_numbers[i] > max_ol) {
						max_ol = ol_num_numbers[i];
						ol_evaluate_val = i;
					}
				}
				assert(ol_evaluate_val != -1);
				if(max_ol > bitmap_ol_num->config->n_merge_threshold) {
					bitmap_ol_num->evaluate(tid, ol_evaluate_val);
					ol_num_numbers[ol_evaluate_val] = 1;
				}
				bitmap_ol_num->trans_commit(tid, glob_manager->fetch_ts() + 1, inserted_max_row_id);				
			}
		}
		else if( query_number == CHBenchQuery::CHBenchQ6 ){
			if(new_order_numbers % 5 == 4) {
				bitmap_q->evaluate(tid, 1);
			}
			bitmap_q->trans_commit(tid, glob_manager->fetch_ts() + 1, inserted_max_row_id);
		}
		else {
			assert(0);
		}
		new_order_numbers++;

		return finish(rc, 1);
	}

	if(CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BTREE_METHOD) {
		for(int i = 0; i < inserted_rows.size(); i++) {
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ1) {
				_wl->index_insert((INDEX *)_wl->i_orderline_d, query->o_entry_d, inserted_rows[i], 0);
			}
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ6) {
				uint64_t key = chbenchQ6Key(inserted_rows_quantity[i], query->o_entry_d);
				_wl->index_insert((INDEX *)_wl->i_orderline, key, inserted_rows[i], 0);
			}
		}
	}

	if(CHBENCH_QUERY_METHOD == CHBenchQueryMethod::BWTREE_METHOD) {
		if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ1) {
			_wl->i_Q1_bwtree->AssignGCID(tid);
		}
		if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ6) {
			_wl->i_Q6_bwtree->AssignGCID(tid);
		}
		for(int i = 0; i < inserted_rows.size(); i++) {
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ1) {
				_wl->index_insert((INDEX *)_wl->i_Q1_bwtree, query->o_entry_d, inserted_rows[i], 0);
			}
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ6) {
				uint64_t key = chbenchQ6Key(inserted_rows_quantity[i], query->o_entry_d);
				_wl->index_insert((INDEX *)_wl->i_Q6_bwtree, key, inserted_rows[i], 0);
			}
		}
		if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ1) {
			_wl->i_Q1_bwtree->UnregisterThread(tid);
		}
		if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ6) {
			_wl->i_Q6_bwtree->UnregisterThread(tid);
		}
	}

	if(CHBENCH_QUERY_METHOD == CHBenchQueryMethod::ART_METHOD) {
		for(int i = 0; i < inserted_rows.size(); i++) {
			uint64_t primary_key = orderlineKey(w_id, d_id, o_id) * 100 + i + 1;
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ1) {
				unique_lock<shared_mutex> w_lock(_wl->i_Q1_art->rw_lock);
				_wl->index_insert_with_primary_key((INDEX *)_wl->i_Q1_art, query->o_entry_d, primary_key,inserted_rows[i], 0);
			}
			if(CHBENCH_QUERY_TYPE == CHBenchQuery::CHBenchQ6) {
				uint64_t key = chbenchQ6Key(inserted_rows_quantity[i], query->o_entry_d);
				unique_lock<shared_mutex> w_lock(_wl->i_Q6_art->rw_lock);
				_wl->index_insert_with_primary_key((INDEX *)_wl->i_Q6_art, key, primary_key,inserted_rows[i], 0);
			}
		}
	}

	assert( rc == RCOK );
	return finish(rc);
}

RC 
chbench_txn_man::run_order_status(chbench_query * query) {
/*	row_t * r_cust;
	if (query->by_last_name) {
		// EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
		// EXEC SQL DECLARE c_name CURSOR FOR SELECT c_balance, c_first, c_middle, c_id
		// FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id ORDER BY c_first;
		// EXEC SQL OPEN c_name;
		// if (namecnt%2) namecnt++; / / Locate midpoint customer for (n=0; n<namecnt/ 2; n++)
		// {
		// 	EXEC SQL FETCH c_name
		// 	INTO :c_balance, :c_first, :c_middle, :c_id;
		// }
		// EXEC SQL CLOSE c_name;

		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		uint64_t thd_id = get_thd_id();
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
	} else {
		// EXEC SQL SELECT c_balance, c_first, c_middle, c_last
		// INTO :c_balance, :c_first, :c_middle, :c_last
		// FROM customer
		// WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
		uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		r_cust = (row_t *) item->location;
	}
#if TPCC_ACCESS_ALL

	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	char * c_first = r_cust_local->get_value(C_FIRST);
	char * c_middle = r_cust_local->get_value(C_MIDDLE);
	char * c_last = r_cust_local->get_value(C_LAST);
#endif
	// EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
	// INTO :o_id, :o_carrier_id, :entdate FROM orders
	// ORDER BY o_id DESC;
	uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
	INDEX * index = _wl->i_order;
	itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
	row_t * r_order = (row_t *) item->location;
	row_t * r_order_local = get_row(r_order, RD);
	if (r_order_local == NULL) {
		assert(false); 
		return finish(Abort);
	}

	uint64_t o_id, o_entry_d, o_carrier_id;
	r_order_local->get_value(O_ID, o_id);
#if TPCC_ACCESS_ALL
	r_order_local->get_value(O_ENTRY_D, o_entry_d);
	r_order_local->get_value(O_CARRIER_ID, o_carrier_id);
#endif
#if DEBUG_ASSERT
	itemid_t * it = item;
	while (it != NULL && it->next != NULL) {
		uint64_t o_id_1, o_id_2;
		((row_t *)it->location)->get_value(O_ID, o_id_1);
		((row_t *)it->next->location)->get_value(O_ID, o_id_2);
		assert(o_id_1 > o_id_2);
	}
#endif

	// EXEC SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// ol_amount, ol_delivery_d
	// FROM order_line
	// WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// EXEC SQL OPEN c_line;
	// EXEC SQL WHENEVER NOT FOUND CONTINUE;
	// i=0;
	// while (sql_notfound(FALSE)) {
	// 		i++;
	//		EXEC SQL FETCH c_line
	//		INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i], :ol_amount[i], :ol_delivery_d[i];
	// }
	key = orderlineKey(query->w_id, query->d_id, o_id);
	index = _wl->i_orderline;
	item = index_read(index, key, wh_to_part(query->w_id));
	assert(item != NULL);
#if TPCC_ACCESS_ALL
	// TODO the rows are simply read without any locking mechanism
	while (item != NULL) {
		row_t * r_orderline = (row_t *) item->location;
		int64_t ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d;
		r_orderline->get_value(OL_I_ID, ol_i_id);
		r_orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
		r_orderline->get_value(OL_QUANTITY, ol_quantity);
		r_orderline->get_value(OL_AMOUNT, ol_amount);
		r_orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
		item = item->next;
	}
#endif

final:
	assert( rc == RCOK );
	return finish(rc)*/
	return RCOK;
}


//TODO concurrency for index related operations is not completely supported yet.
// In correct states may happen with the current code.

RC 
chbench_txn_man::run_delivery(chbench_query * query) {
/*
	// XXX HACK if another delivery txn is running on this warehouse, simply commit.
	if ( !ATOM_CAS(_wl->delivering[query->w_id], false, true) )
		return finish(RCOK);

	for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
		uint64_t key = distKey(d_id, query->w_id);
		INDEX * index = _wl->i_orderline_wd;
		itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
		assert(item != NULL);
		while (item->next != NULL) {
#if DEBUG_ASSERT
			uint64_t o_id_1, o_id_2;
			((row_t *)item->location)->get_value(OL_O_ID, o_id_1);
			((row_t *)item->next->location)->get_value(OL_O_ID, o_id_2);
			assert(o_id_1 > o_id_2);
#endif
			item = item->next;
		}
		uint64_t no_o_id;
		row_t * r_orderline = (row_t *)item->location;
		r_orderling->get_value(OL_O_ID, no_o_id);
		// TODO the orderline row should be removed from the table and indexes.
		
		index = _wl->i_order;
		key = orderPrimaryKey(query->w_id, d_id, no_o_id);
		itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
		row_t * r_order = (row_t *)item->location;
		row_t * r_order_local = get_row(r_order, WR);

		uint64_t o_c_id;
		r_order_local->get_value(O_C_ID, o_c_id);
		r_order_local->set_value(O_CARRIER_ID, query->o_carrier_id);

		item = index_read(_wl->i_order_line, orderlineKey(query->w_id, d_id, no_o_id));
		double sum_ol_amount;
		double ol_amount;
		while (item != NULL) {
			// TODO the row is not locked
			row_t * r_orderline = (row_t *)item->location;
			r_orderline->set_value(OL_DELIVERY_D, query->ol_delivery_d);
			r_orderline->get_value(OL_AMOUNT, ol_amount);
			sum_ol_amount += ol_amount;
		}
		
		key = custKey(o_c_id, d_id, query->w_id);
		itemid_t * item = index_read(_wl->i_customer_id, key, wh_to_part(query->w_id));
		row_t * r_cust = (row_t *)item->location;
		double c_balance;
		uint64_t c_delivery_cnt;
	}
*/
	return RCOK;
}

RC 
chbench_txn_man::run_stock_level(chbench_query * query) {
	return RCOK;
}

void chbench_txn_man::run_Q6_scan_singlethread(uint64_t start_row, uint64_t end_row, chbench_query * query, std::tuple<double, int> &result, int wid) {

	set_affinity(wid);

	// define constant condition
	uint64_t min_delivery_d = query->min_delivery_d;
	uint64_t max_delivery_d = query->max_delivery_d;
	int64_t min_quantity = query->min_quantity;
	int64_t max_quantity = query->max_quantity;
	double current_amount_t = 0;
	int current_amount_cnt = 0;

	row_t* current_row;

	for(uint64_t row_id = start_row; row_id < end_row; row_id++) {
		current_row = (row_t*) &(_wl->t_orderline->row_buffer[row_id]);
		assert(current_row != NULL);
		row_t* row_local = get_row(current_row, SCAN);
		if(row_local == NULL) {
			continue;
		}
		
		// extract artributions
		uint64_t current_delivery_d;
		int64_t current_quantity;
		row_local->get_value(OL_DELIVERY_D, current_delivery_d);
		row_local->get_value(OL_QUANTITY, current_quantity);

		// check condition
		if( current_delivery_d >= min_delivery_d && current_delivery_d < max_delivery_d && \
			current_quantity >= min_quantity && current_quantity <= max_quantity ) {
			double current_amount;
			row_local->get_value(OL_AMOUNT, current_amount);
			current_amount_t += current_amount;
			current_amount_cnt ++;
		} 
	}

	std::get<0>(result) = current_amount_t;
	std::get<1>(result) = current_amount_cnt;

	return;
}

RC chbench_txn_man::run_Q6_scan(int tid, chbench_query * query) {
	/*
		select	sum(ol_amount) as revenue
		from	orderline
		where	ol_delivery_d >= '1999-01-01 00:00:00.000000'
				and ol_delivery_d < '2020-01-01 00:00:00.000000'
				and ol_quantity between 1 and 100000
	*/


	RC rc = RCOK;
	
	double revenue = 0;
	int cnt = 0;

	// does here need atomic?
	uint64_t max_orderlines = (uint64_t)_wl->t_orderline->cur_tab_size;


	auto start = std::chrono::high_resolution_clock::now();

	std::vector<std::tuple<double, int>> results(CHBENCH_Q6_SCAN_THREADS);
	uint64_t block_size = max_orderlines / CHBENCH_Q6_SCAN_THREADS;
	uint64_t block_start = 0;
	std::vector<std::thread> threads(CHBENCH_Q6_SCAN_THREADS - 1);

	for (int i = 0; i < CHBENCH_Q6_SCAN_THREADS - 1; i++) {
		threads[i] = std::thread(&chbench_txn_man::run_Q6_scan_singlethread, *this,\
								block_start, block_start + block_size, query, std::ref(results[i]), tid*CHBENCH_Q6_SCAN_THREADS+i);
		block_start = block_start + block_size;
	}
	run_Q6_scan_singlethread(block_start, max_orderlines, query, std::ref(results[CHBENCH_Q6_SCAN_THREADS - 1]), tid*CHBENCH_Q6_SCAN_THREADS+CHBENCH_Q6_SCAN_THREADS - 1);

	for (auto &thread : threads) {
		thread.join();
	}
	auto read_end = std::chrono::high_resolution_clock::now();
	for (auto &result : results) {
		revenue += std::get<0>(result);
		cnt += std::get<1>(result);
	}

	auto end = std::chrono::high_resolution_clock::now();
	long  long time_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
	long  long read_time = std::chrono::duration_cast<std::chrono::microseconds>(read_end-start).count();
//	if (perf_enabled == true && tid == 0) {
//		kill_perf_process(perf_pid);
//		usleep(WAIT_FOR_PERF_U);
//	}

	string tmp = "Q6 SCAN-Paral(ms): " + to_string(time_elapsed_us/1000)+"\t\t"+"read_time:"+to_string(read_time/1000)+"\t\t\t"+"count_time(ms):"+to_string((time_elapsed_us-read_time)/1000) +"\t\t"+"nums:"+to_string(cnt)+"\t\t"+"revenue:"+to_string(revenue) +"\n";
	output_info[tid].push_back(tmp);
	assert(rc == RCOK);
	return finish(rc);
	

	// return RCOK;
}



RC chbench_txn_man::run_Q6_btree(int tid, chbench_query * query) {

	uint64_t min_delivery_d = query->min_delivery_d;
	uint64_t max_delivery_d = query->max_delivery_d;
	int64_t min_quantity = query->min_quantity;
	int64_t max_quantity = query->max_quantity;

	RC rc = RCOK;
	
	double revenue = 0;
	int cnt = 0;
	row_t* current_row;

	long long total_us = (long long)0;
	vector<itemid_t *> item_list{};

	INDEX* index = _wl->i_orderline;

	auto start = std::chrono::high_resolution_clock::now();
	shared_lock<shared_mutex> r_lock(index->rw_lock);

	Date r_date(max_delivery_d / 10000, (max_delivery_d % 10000) / 100, max_delivery_d % 100);
	Date l_date(min_delivery_d / 10000, (min_delivery_d % 10000) / 100, min_delivery_d % 100);

	uint64_t curr_key = 0;
	uint64_t quantity = min_quantity;	

	while(quantity <= max_quantity) {

		for(Date date = l_date; date < r_date; date++) {

			uint64_t int_date = date.DateToUint64();

			uint64_t key = chbenchQ6Key(quantity, int_date);

			if ( !index->index_exist(key, 0) ) {
					continue;
			}
			itemid_t * item = index_read(index, key, 0);
			while(item) {
				uint64_t curr_qunantity = key / 100000000;
				uint64_t curr_date = key % 100000000;
				if(curr_date >= min_delivery_d && curr_date < max_delivery_d && curr_qunantity >= min_quantity && curr_qunantity <= max_quantity) {

				}
				else {
					if(curr_qunantity != quantity) {
						if(curr_date >= max_delivery_d) {
							quantity = curr_qunantity;
						}
						else {
							quantity = curr_qunantity - 1;
						}
					}
					break;
				}
				for (itemid_t * local_item = item; local_item != NULL; local_item = local_item->next) {
					item_list.push_back(local_item);
				}
				index->index_next(get_thd_id(), item, key);
			}
			break;
		}
		quantity++;
	}

	auto index_end = std::chrono::high_resolution_clock::now();
	auto index_us = std::chrono::duration_cast<std::chrono::microseconds>(index_end - start).count();
	r_lock.unlock();

	for (auto const &local_item : item_list)
	{
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		cnt ++;
		// cout << "address = " << &r_lt_local->data << endl;
		double current_amount;
		r_lt_local->get_value(OL_AMOUNT, current_amount);
		revenue += current_amount;
	}

	auto end = std::chrono::high_resolution_clock::now();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	string tmp = "Q6 BTree(ms): " + to_string(total_us/1000)+"\t\t" + "index_read(ms): " + to_string(index_us/1000)+"\t\t" +"count_time(ms):"+to_string((total_us-index_us)/1000)+"\t\t"+"nums:"+to_string(cnt)+ "\t\t"+"revenue:"+to_string(revenue)+"\n";
	output_info[tid].push_back(tmp);
	

	assert(rc == RCOK);
	return finish(rc);
}


RC chbench_txn_man::run_Q6_bitmap(int tid, chbench_query * query) {

	uint64_t min_delivery_d = query->min_delivery_d;
	uint64_t max_delivery_d = query->max_delivery_d;
	int64_t min_quantity = query->min_quantity;
	int64_t max_quantity = query->max_quantity;

	RC rc = RCOK;
	
	double revenue = 0;
	int cnt = 0;

	auto start = std::chrono::high_resolution_clock::now();

	rabit::Rabit *bitmap_dd, *bitmap_qt;
	bitmap_dd = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_deliverydate);
	bitmap_qt = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_quantity);
	ibis::bitvector result;
	
	get_bitvector_result(result, bitmap_dd, bitmap_qt, 1, 1);

	row_t *row_buffer = _wl->t_orderline->row_buffer;

	// selectivity 1.8%
	uint64_t n_ids_max = _wl->t_orderline->cur_tab_size * 0.05;
	int *ids = new int[n_ids_max];

	// Convert bitvector to ID list
	for (ibis::bitvector::indexSet is = result.firstIndexSet(); is.nIndices() > 0; ++ is) 
	{
		const ibis::bitvector::word_t *ii = is.indices();
		if (is.isRange()) {
			for (ibis::bitvector::word_t j = *ii;
					j < ii[1];
					++ j) {
				ids[cnt] = j;
				++ cnt;
			}
		}
		else {
			for (unsigned j = 0; j < is.nIndices(); ++ j) {
				ids[cnt] = ii[j];
				++ cnt;
			}
		}
	}

	auto index_end = std::chrono::high_resolution_clock::now();
	auto index_us = std::chrono::duration_cast<std::chrono::microseconds>(index_end-start).count();

	int deletecnt = 0;
	// Fetch tuples in ID list
	for(int k = 0; k < cnt; k++) 
	{
		row_t *row_tmp = (row_t *) &row_buffer[ids[k]];
		row_t *row_local = get_row(row_tmp, SCAN);
		if (row_local == NULL) {
			deletecnt++;
			// return finish(Abort);
			continue;
		}
		double current_amount;
		row_local->get_value(OL_AMOUNT, current_amount);
		revenue += current_amount; 
	}
	
	auto end = std::chrono::high_resolution_clock::now();
	long long total_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

	string tmp = "Q6 Bitmap (ms): " + to_string(total_us/1000)+ "\t\t"+"index_read(ms): " + to_string(index_us/1000)+"\t\t"+"count_time(ms):"+to_string((total_us-index_us)/1000)+"\t\t" + "nums: " + to_string(cnt)+"\t\t" +"revenue:"+to_string(revenue) +"\n";
	output_info[tid].push_back(tmp);

	delete [] ids;
	assert(rc == RCOK);
	return finish(rc, 0);
}

void chbench_txn_man::run_Q6_bitmap_singlethread(SegBtv &seg_btv1, SegBtv &seg_btv2, int begin, int end, pair<double, int> &result)
{
	// selectivity 1.8%, read by four thread. so, 10% size of table is ok.
	row_t *row_buffer = _wl->t_orderline->row_buffer;
	seg_btv1._and_in_thread(&seg_btv2, begin, end);
	uint64_t n_ids_max = _wl->t_orderline->cur_tab_size * 0.1;
	int *ids = new int[n_ids_max];
	// Convert bitvector to ID list
	double &revenue = result.first;
	revenue = 0;
	int &cnt = result.second;
	cnt = 0;
	auto iter1 = seg_btv1.seg_table.find(begin);
	auto iter2 = seg_btv1.seg_table.find(end);
	for(; iter1 != iter2; iter1++) {
		ibis::bitvector *current_result = iter1->second->btv;
	for (ibis::bitvector::indexSet is = current_result->firstIndexSet(); is.nIndices() > 0; ++ is) 
	{
		const ibis::bitvector::word_t *ii = is.indices();
		if (is.isRange()) {
			for (ibis::bitvector::word_t j = *ii;
					j < ii[1];
					++ j) {
				ids[cnt] = j;
				++ cnt;
			}
		}
		else {
			for (unsigned j = 0; j < is.nIndices(); ++ j) {
				ids[cnt] = ii[j];
				++ cnt;
			}
		}
	}
	}
	int deletecnt = 0;
	// Fetch tuples in ID list
	for(int k = 0; k < cnt; k++) 
	{
		row_t *row_tmp = (row_t *) &row_buffer[ids[k]];
		row_t *row_local = get_row(row_tmp, SCAN);
		if (row_local == NULL) {
			deletecnt++;
			// return finish(Abort);
			continue;
		}
		cnt -= deletecnt;
		double current_amount;
		row_local->get_value(OL_AMOUNT, current_amount);
		revenue += current_amount; 
	}

	delete [] ids;
}

void chbench_txn_man::bitmap_singlethread_fetch(int begin, int end, pair<double, int> &result, row_t *row_buffer, int *ids, int wid)
{
	set_affinity(wid);

	// Fetch tuples in ID list
	double current_amount_t = 0;
	int delete_cnt = 0;
	result.first = 0;
	result.second = 0;
	for(int k = begin; k < end; k++) 
	{
		row_t *row_tmp = (row_t *) &row_buffer[ids[k]];
		row_t *row_local = get_row(row_tmp, SCAN);
		if (row_local == NULL) {
			delete_cnt++;
			// return finish(Abort);
			continue;
		}
		double current_amount;
		row_local->get_value(OL_AMOUNT, current_amount);
		current_amount_t += current_amount; 
	}
	
	result.first = current_amount_t;
	result.second = delete_cnt;
}

RC chbench_txn_man::run_Q6_bitmap_parallel(int tid, chbench_query * query) {

	uint64_t min_delivery_d = query->min_delivery_d;
	uint64_t max_delivery_d = query->max_delivery_d;
	int64_t min_quantity = query->min_quantity;
	int64_t max_quantity = query->max_quantity;

	RC rc = RCOK;
	
	double revenue = 0;
	int cnt = 0;

	auto start = std::chrono::high_resolution_clock::now();

	rabit::Rabit *bitmap_dd, *bitmap_qt;

	bitmap_dd = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_deliverydate);
	bitmap_qt = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q6_quantity);

	ibis::bitvector result;
	get_bitvector_result(result, bitmap_dd, bitmap_qt, 1, 1);
	
	row_t *row_buffer = _wl->t_orderline->row_buffer;

	// selectivity 1.8%
	uint64_t n_ids_max = _wl->t_orderline->cur_tab_size * 0.3;
	int *ids = new int[n_ids_max];

	// Convert bitvector to ID list
	for (ibis::bitvector::indexSet is = result.firstIndexSet(); is.nIndices() > 0; ++ is) 
	{
		const ibis::bitvector::word_t *ii = is.indices();
		if (is.isRange()) {
			for (ibis::bitvector::word_t j = *ii;
					j < ii[1];
					++ j) {
				ids[cnt] = j;
				++ cnt;
			}
		}
		else {
			for (unsigned j = 0; j < is.nIndices(); ++ j) {
				ids[cnt] = ii[j];
				++ cnt;
			}
		}
	}
	auto index_end = std::chrono::high_resolution_clock::now();
	// parallel fetch
	int n_threads = bitmap_dd->config->nThreads_for_getval;
	assert(bitmap_dd->config->nThreads_for_getval == CHBENCH_Q6_SCAN_THREADS);
	int n_cnt_per_thread = cnt / n_threads;
	int n_left = cnt % n_threads;

	std::vector<std::thread> threads(n_threads);
	vector<int> begin(n_threads + 1, 0);
	vector<pair<double, int>> answer(n_threads);

	for (int i = 1; i <= n_left; i++)
		begin[i] = begin[i - 1] + n_cnt_per_thread + 1;
	for (int i = n_left + 1; i <= n_threads; i++)
		begin[i] = begin[i - 1] + n_cnt_per_thread;

	for (int i = 0; i < n_threads; i++) {
		threads[i] = thread(&chbench_txn_man::bitmap_singlethread_fetch, *this, begin[i], begin[i + 1], \
							std::ref(answer[i]), row_buffer, ids, tid*CHBENCH_Q6_SCAN_THREADS + i);
	}
	for (int i = 0; i < n_threads; i++) {
		threads[i].join();
	}

	for(int i = 0; i < answer.size(); i++) {
		revenue += answer[i].first;
		cnt -= answer[i].second;
	}

	auto end = std::chrono::high_resolution_clock::now();
	long long total_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
	long long index_us = std::chrono::duration_cast<std::chrono::microseconds>(index_end-start).count();

	string tmp = "Q6 Bitmap-Parallel(ms):" + to_string(total_us/1000)+"\t"+"index_time(ms):"+to_string(index_us/1000)+"\t\t"+"count_time(ms):"+to_string((total_us-index_us)/1000)+"\t\t" + "nums: " + to_string(cnt)+"\t\t"+"revenue:"+to_string(revenue) + "\n";
	output_info[tid].push_back(tmp);

	delete [] ids;
	assert(rc == RCOK);
	return finish(rc, 0);
}

RC chbench_txn_man::run_Q6_bwtree(int tid, chbench_query * query)
{

	uint64_t min_delivery_d = query->min_delivery_d;
	uint64_t max_delivery_d = query->max_delivery_d;
	int64_t min_quantity = query->min_quantity;
	int64_t max_quantity = query->max_quantity;

	RC rc = RCOK;
	
	double revenue = 0;
	int cnt = 0;
	row_t* current_row;

	long long total_us = (long long)0;
	long long index_read_time=(long long)0;
	vector<itemid_t *> item_list{};

	index_bwtree *index = _wl->i_Q6_bwtree;

	auto start = std::chrono::high_resolution_clock::now();
	auto index_read_start = std::chrono::high_resolution_clock::now();

	// int perf_pid;
	// if (perf_enabled == true && tid == 0) {
	// 	perf_pid = gen_perf_process((char *)"BWTREE");
	// 	usleep(WAIT_FOR_PERF_U);
	// }

	index->AssignGCID(tid);
	Date r_date(max_delivery_d / 10000, (max_delivery_d % 10000) / 100, max_delivery_d % 100);
	Date l_date(min_delivery_d / 10000, (min_delivery_d % 10000) / 100, min_delivery_d % 100);
	for(; l_date < r_date; l_date++) {
		uint64_t int_date = l_date.DateToUint64();
		for(int64_t quantity = min_quantity; quantity <= max_quantity; quantity++) {
			uint64_t key = chbenchQ6Key(quantity, int_date);
			if ( !index->index_exist(key, 0) ){
					continue;
				}
				auto start1 = std::chrono::high_resolution_clock::now();
				vector<itemid_t *> items = index_read(index, key, 0);
				for (auto item : items) {
					item_list.push_back(item);
				}
		}
	}
	index->UnregisterThread(tid);

	// if (perf_enabled == true && tid == 0) {
	// 	kill_perf_process(perf_pid);
	// }
	auto index_read_end = std::chrono::high_resolution_clock::now();

	for (auto const &local_item : item_list)
	{
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		cnt ++;
		// cout << "address = " << &r_lt_local->data << endl;
		double current_amount;
		r_lt_local->get_value(OL_AMOUNT, current_amount);
		revenue += current_amount;
	}

	auto end = std::chrono::high_resolution_clock::now();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
	index_read_time=std::chrono::duration_cast<std::chrono::microseconds>(index_read_end - index_read_start).count();

	string tmp = "Q6 BWTree(ms): " + to_string(total_us/1000)+"\t\t"+"index_read(ms):"+to_string(index_read_time/1000)+"\t\t"+"count_time(ms):"+to_string((total_us-index_read_time)/1000)+"\t\t"+"nums:"+to_string(cnt)+"\t\t"+"revenue:"+to_string(revenue)+"\n";
	output_info[tid].push_back(tmp);
	
	assert(rc == RCOK);
	return finish(rc);
}

RC chbench_txn_man::run_Q6_art(int tid, chbench_query * query) {

	uint64_t min_delivery_d = query->min_delivery_d;
	uint64_t max_delivery_d = query->max_delivery_d;
	int64_t min_quantity = query->min_quantity;
	int64_t max_quantity = query->max_quantity;

	RC rc = RCOK;
	
	double revenue = 0;
	int cnt = 0;
	row_t* current_row;

	long long total_us = (long long)0;
	size_t item_list_size = _wl->t_orderline->cur_tab_size * 0.05;
	size_t item_list_used = 0;
	itemid_t **item_list = new itemid_t*[item_list_size];

	index_art *index = _wl->i_Q6_art;

	auto start = std::chrono::high_resolution_clock::now();
	
	shared_lock<shared_mutex> r_lock(index->rw_lock);
	Date r_date(max_delivery_d / 10000, (max_delivery_d % 10000) / 100, max_delivery_d % 100);
	Date l_date(min_delivery_d / 10000, (min_delivery_d % 10000) / 100, min_delivery_d % 100);

	for(int64_t quantity = min_quantity; quantity <= max_quantity; quantity++) {
		uint64_t startKey = chbenchQ6Key(quantity, l_date.DateToUint64());
		uint64_t endKey = chbenchQ6Key(quantity, r_date.DateToUint64() - 1);
		size_t itemsfound;
		index->lookup_range(startKey, endKey, 0, item_list + item_list_used, item_list_size - item_list_used, itemsfound);
		int i = item_list_used;
		item_list_used += itemsfound;
		itemsfound = item_list_used;
		for(; i < itemsfound; i++) {
			itemid_t *local_item = item_list[i];
			local_item = local_item->next;
			while(local_item) {
				item_list[item_list_used++] = local_item;
				local_item = local_item->next;
			}		
		}
	}

	auto index_end = std::chrono::high_resolution_clock::now();
	auto index_us = std::chrono::duration_cast<std::chrono::microseconds>(index_end - start).count();
	r_lock.unlock();

	for (size_t i = 0; i < item_list_used; i++)
	{
		itemid_t *local_item = item_list[i];
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		cnt ++;
		// cout << "address = " << &r_lt_local->data << endl;
		double current_amount;
		r_lt_local->get_value(OL_AMOUNT, current_amount);
		revenue += current_amount;
	}

	auto end = std::chrono::high_resolution_clock::now();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	string tmp = "Q6 ART(ms): " + to_string(total_us/1000) +"\t\t\t"+ "index_read(ms): " + to_string(index_us/1000)+"\t\t" +"count_time(ms):"+to_string((total_us-index_us)/1000)+"\t\t"+ "nums:" + to_string(cnt)+"\t\t" +"revenue:"+to_string(revenue) +"\n";
	output_info[tid].push_back(tmp);
	
	delete item_list;
	assert(rc == RCOK);
	return finish(rc);
}

void chbench_txn_man::get_bitvector_result(ibis::bitvector &result, rabit::Rabit *rabit1, rabit::Rabit *rabit2, int rabit1_pos, int rabit2_pos)
{
	Bitvector *bitmap1 = rabit1->Btvs[rabit1_pos];
	Bitvector *bitmap2;
	if(rabit2)
		bitmap2 = rabit2->Btvs[rabit2_pos];

	result.copy(*bitmap1->btv);
	TransDesc *trans = bitmap1->log_shortcut;
	trans = trans->next;
	while(trans && trans->l_commit_ts < get_ts()) {
		for(auto rub : trans->rubs) {
			// if(rub.second.pos.begin()->first == (uint32_t)rabit1_pos) {
			result.setBit(rub.second.row_id, 1, rabit1->config);
			// }
		}
		trans = trans->next;
	}

	if(!rabit2) {
		return;
	}

	ibis::bitvector tmp;
	tmp.copy(*bitmap2->btv);
	trans = bitmap2->log_shortcut;
	trans = trans->next;
	while(trans && trans->l_commit_ts < get_ts()) {
		for(auto rub : trans->rubs) {
			// if(rub.second.pos.begin()->first == (uint32_t)rabit2_pos) {
			tmp.setBit(rub.second.row_id, 1, rabit2->config);
			// }
		}
		trans = trans->next;
	}

	result &= tmp;
}


void chbench_txn_man::q1_add_answer(row_t* row_local, chbench_q1_data &result) {
	uint32_t current_number;
	int64_t current_quantity;
	double current_amount;
	row_local->get_value(OL_NUMBER, current_number);
	row_local->get_value(OL_AMOUNT, current_amount);
	row_local->get_value(OL_QUANTITY, current_quantity);
	result.cnt[current_number]++;
	result.sum_amount[current_number] += current_amount;
	result.sum_quantity[current_number] += (uint64_t)current_quantity;
	return;
}

void chbench_txn_man::run_Q1_scan_singlethread(uint64_t start_row, uint64_t end_row, chbench_query * query, chbench_q1_data &result, int wid) {
	
	set_affinity(wid);

	// define constant condition
	uint64_t min_delivery_d = query->min_delivery_d;

	row_t* current_row;
	chbench_q1_data current_data(result.d_size);

	for(uint64_t row_id = start_row; row_id < end_row; row_id++) {
		current_row = (row_t*) &(_wl->t_orderline->row_buffer[row_id]);
		assert(current_row != NULL);
		row_t* row_local = get_row(current_row, SCAN);
		if(row_local == NULL) {
			continue;
		}
		
		// extract artributions
		uint64_t current_delivery_d;
		row_local->get_value(OL_DELIVERY_D, current_delivery_d);

		// check condition
		if( current_delivery_d >= min_delivery_d) {
			q1_add_answer(row_local, current_data);
		} 
	}
	result = current_data;
	return;
}


RC chbench_txn_man::run_Q1_scan(int tid, chbench_query * query) {
	/*
	select  ol_number,
	 	sum(ol_quantity) as sum_qty,
	 	sum(ol_amount) as sum_amount,
	 	avg(ol_quantity) as avg_qty,
	 	avg(ol_amount) as avg_amount,
	 	count(*) as count_order
	from	 orderline
	where	 ol_delivery_d > '2007-01-02 00:00:00.000000'
	group by ol_number order by ol_number
	*/


	RC rc = RCOK;

	uint64_t sum_quantity = 0;
	double sum_amount = 0;
	
	// does here need atomic?
	uint64_t max_orderlines = (uint64_t)_wl->t_orderline->cur_tab_size;

	auto start = std::chrono::high_resolution_clock::now();

	std::vector<chbench_q1_data> results(CHBENCH_Q6_SCAN_THREADS, 16);
	uint64_t block_size = max_orderlines / CHBENCH_Q6_SCAN_THREADS;
	uint64_t block_start = 0;
	std::vector<std::thread> threads(CHBENCH_Q6_SCAN_THREADS - 1);

	for (int i = 0; i < CHBENCH_Q6_SCAN_THREADS - 1; i++) {
		threads[i] = std::thread(&chbench_txn_man::run_Q1_scan_singlethread, *this,\
								block_start, block_start + block_size, query, std::ref(results[i]), tid*CHBENCH_Q6_SCAN_THREADS+i);
		block_start = block_start + block_size;
	}
	run_Q1_scan_singlethread(block_start, max_orderlines, query, std::ref(results[CHBENCH_Q6_SCAN_THREADS - 1]), tid*CHBENCH_Q6_SCAN_THREADS+CHBENCH_Q6_SCAN_THREADS - 1);

	for (auto &thread : threads) {
		thread.join();
	}

	for(int i = 0; i < CHBENCH_Q6_SCAN_THREADS - 1; i++) {
		results[CHBENCH_Q6_SCAN_THREADS - 1] += results[i];
	}

	for(int i = 0; i < results[CHBENCH_Q6_SCAN_THREADS - 1].d_size; i++) {
		chbench_q1_data* tmp = &results[CHBENCH_Q6_SCAN_THREADS - 1];
		if(tmp->cnt[i] > 0) {
			tmp->avg_amount[i] = tmp->sum_amount[i] / tmp->cnt[i];
			tmp->avg_quantity[i] = tmp->sum_quantity[i] / (double)tmp->cnt[i];
		}
	}
	auto end = std::chrono::high_resolution_clock::now();
	long  long time_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

//	if (perf_enabled == true && tid == 0) {
//		kill_perf_process(perf_pid);
//		usleep(WAIT_FOR_PERF_U);
//	}

	string tmp = "Q1 Scan (parallel)(ms): " + to_string(time_elapsed_us/1000) + "\n";
	output_info[tid].push_back(tmp);
	assert(rc == RCOK);
	return finish(rc);
	

	// return RCOK;
}

RC chbench_txn_man::run_Q1_btree(int tid, chbench_query * query) {

	uint64_t min_delivery_d = query->min_delivery_d;

	RC rc = RCOK;
	
	chbench_q1_data ans(16);
	row_t* current_row;

	long long total_us = (long long)0;
	vector<itemid_t *> item_list{};

	INDEX* index = _wl->i_orderline_d;

	auto start = std::chrono::high_resolution_clock::now();
	shared_lock<shared_mutex> r_lock(index->rw_lock);
	time_t curtime;
	time(&curtime);
	tm *nowtime = localtime(&curtime);
	Date curr_date(nowtime->tm_year + 1900, nowtime->tm_mon + 1, nowtime->tm_mday);

	for(Date date = Date(2007, 1, 2) ; date <= curr_date; date++) {
			uint64_t key = date.DateToUint64();
			if ( !index->index_exist(key, 0) ){
					continue;
				}		
			itemid_t * item = index_read(index, key, 0);
			while(item) {
				for (itemid_t * local_item = item; local_item != NULL; local_item = local_item->next) {
					item_list.push_back(local_item);
				}
				index->index_next(get_thd_id(), item, key);
			}
			break;	
	}

	auto index_end = std::chrono::high_resolution_clock::now();
	auto index_us = std::chrono::duration_cast<std::chrono::microseconds>(index_end - start).count();;
	for (auto const &local_item : item_list)
	{
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		// cout << "address = " << &r_lt_local->data << endl;
		q1_add_answer(r_lt_local, ans);
	}

	for(int i = 0; i < ans.d_size; i++) {
		if(ans.cnt[i] == 0) {
			continue;
		}
		ans.avg_amount[i] = ans.sum_amount[i] / ans.cnt[i];
		ans.avg_quantity[i] = ans.sum_quantity[i] / (double)ans.cnt[i];
	}

	auto end = std::chrono::high_resolution_clock::now();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	
	string tmp = "Q1 Btree (ms): " + to_string(total_us/1000) + " index_read(ms): " + to_string(index_us/1000) + " nums: " + to_string(ans.cnt[1]) + "\n";
	output_info[tid].push_back(tmp);
	

	assert(rc == RCOK);
	return finish(rc);
}

RC chbench_txn_man::run_Q1_bitmap(int tid, chbench_query * query) {
	uint64_t min_delivery_d = query->min_delivery_d;

	RC rc = RCOK;
	
	chbench_q1_data ans(16);
	int cnt = 0;

	auto start = std::chrono::high_resolution_clock::now();

	rabit::Rabit *bitmap_dd;
	bitmap_dd = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q1_deliverydate);

	ibis::bitvector result;
	get_bitvector_result(result, bitmap_dd, nullptr, 1, 0);

	row_t *row_buffer = _wl->t_orderline->row_buffer;

	// selectivity 36%
	uint64_t n_ids_max = _wl->t_orderline->cur_tab_size * 0.5;
	int *ids = new int[n_ids_max];

	// Convert bitvector to ID list
	for (ibis::bitvector::indexSet is = result.firstIndexSet(); is.nIndices() > 0; ++ is) 
	{
		const ibis::bitvector::word_t *ii = is.indices();
		if (is.isRange()) {
			for (ibis::bitvector::word_t j = *ii;
					j < ii[1];
					++ j) {
				ids[cnt] = j;
				++ cnt;
			}
		}
		else {
			for (unsigned j = 0; j < is.nIndices(); ++ j) {
				ids[cnt] = ii[j];
				++ cnt;
			}
		}
	}

	auto index_end = std::chrono::high_resolution_clock::now();
	auto index_us = std::chrono::duration_cast<std::chrono::microseconds>(index_end-start).count();

	int deletecnt = 0;
	// Fetch tuples in ID list
	for(int k = 0; k < cnt; k++) 
	{
		row_t *row_tmp = (row_t *) &row_buffer[ids[k]];
		row_t *row_local = get_row(row_tmp, SCAN);
		if (row_local == NULL) {
			deletecnt++;
			// return finish(Abort);
			continue;
		}
		q1_add_answer(row_local, ans);
	}

	for(int i = 0; i < ans.d_size; i++) {
		if(ans.cnt[i] == 0) {
			continue;
		}
		ans.avg_amount[i] = ans.sum_amount[i] / ans.cnt[i];
		ans.avg_quantity[i] = ans.sum_quantity[i] / (double)ans.cnt[i];
	}

	auto end = std::chrono::high_resolution_clock::now();
	long long total_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

	string tmp = "Q1 Bitmap (ms): " + to_string(total_us/1000) + " index_read(ms): " + to_string(index_us/1000) + " nums: " + to_string(ans.cnt[1]) + "\n";
	output_info[tid].push_back(tmp);

	delete [] ids;
	assert(rc == RCOK);
	return finish(rc);
}

void chbench_txn_man::run_Q1_bitmap_fetch_singlethread(int number, rabit::Rabit *bitmap_d, rabit::Rabit *bitmap_number, chbench_q1_data & ans, int wid) {

	set_affinity(wid);
	// selectivity 36% 
	// max > 36%/4
	uint64_t n_ids_max = _wl->t_orderline->cur_tab_size * 0.15;
	int *ids = new int[n_ids_max];
	row_t *row_buffer = _wl->t_orderline->row_buffer;
	
	// work for per thread
	vector<int> ol_numbers;
	ol_numbers.push_back(number);
	ol_numbers.push_back(4 + number);
	ol_numbers.push_back(11 - number);
	if(number == 0) {
		ol_numbers.push_back(12);
	}
	else if(number == 1) {
		ol_numbers.push_back(13);
	}
	else if(number == 2) {
		ol_numbers.push_back(14);
	}
	
	for(int i = 0; i < ol_numbers.size(); i++) {
		number = ol_numbers[i];
		int cnt = 0;
		ibis::bitvector result;
		get_bitvector_result(result, bitmap_d, bitmap_number, 1, number);

		// Convert bitvector to ID list
		for (ibis::bitvector::indexSet is = result.firstIndexSet(); is.nIndices() > 0; ++ is) 
		{
			const ibis::bitvector::word_t *ii = is.indices();
			if (is.isRange()) {
				for (ibis::bitvector::word_t j = *ii;
						j < ii[1];
						++ j) {
					ids[cnt] = j;
					++ cnt;
				}
			}
			else {
				for (unsigned j = 0; j < is.nIndices(); ++ j) {
					ids[cnt] = ii[j];
					++ cnt;
				}
			}
		}
		int deletecnt = 0;

		// auto start = std::chrono::high_resolution_clock::now();
		// Fetch tuples in ID list
		for(int k = 0; k < cnt; k++) 
		{
			row_t *row_tmp = (row_t *) &row_buffer[ids[k]];
			row_t *row_local = get_row(row_tmp, SCAN);
			if (row_local == NULL) {
				deletecnt++;
				// return finish(Abort);
				continue;
			}
			q1_add_answer(row_local, ans);
		}
		/*
		auto end = std::chrono::high_resolution_clock::now();
		if(number == 1) {
			cout << "fetch tuple time is " << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << "us" << endl;
		}
		*/
		
		if(ans.cnt[number] != 0) {
			ans.avg_amount[number] = ans.sum_amount[number] / ans.cnt[number];
			ans.avg_quantity[number] = ans.sum_quantity[number] / (double)ans.cnt[number];
		}
	}

	delete [] ids;

}

RC chbench_txn_man::run_Q1_bitmap_parallel_fetch(int tid, chbench_query * query) {
	uint64_t min_delivery_d = query->min_delivery_d;

	RC rc = RCOK;
	
	chbench_q1_data ans(16);
	int cnt = 0;
	rabit::Rabit *bitmap_d, *bitmap_number;
	bitmap_d = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q1_deliverydate);
	bitmap_number = dynamic_cast<rabit::Rabit *>(_wl->bitmap_q1_ol_number);

	auto start = std::chrono::high_resolution_clock::now();

	std::vector<std::thread> threads(CHBENCH_Q6_SCAN_THREADS);
	for(int i = 0; i < CHBENCH_Q6_SCAN_THREADS; i++) {
		threads[i] = std::thread(&chbench_txn_man::run_Q1_bitmap_fetch_singlethread, *this, \
		i, bitmap_d, bitmap_number, std::ref(ans), tid*CHBENCH_Q6_SCAN_THREADS + i);
	}
	for (auto &thread : threads) {
		thread.join();
	}

	auto end = std::chrono::high_resolution_clock::now();
	long long total_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

	string tmp = "Q1 Bitmap (parallel)(ms): " + to_string(total_us/1000) + " nums: " + to_string(ans.cnt[1]) + "\n";
	output_info[tid].push_back(tmp);

	assert(rc == RCOK);
	return finish(rc);
}

RC chbench_txn_man::run_Q1_bwtree(int tid, chbench_query * query)
{

	uint64_t min_delivery_d = query->min_delivery_d;

	RC rc = RCOK;
	
	chbench_q1_data ans(16);
	row_t* current_row;

	long long total_us = (long long)0;
	vector<itemid_t *> item_list{};

	index_bwtree* index = _wl->i_Q1_bwtree;

	auto start = std::chrono::high_resolution_clock::now();
	index->AssignGCID(tid);
	time_t curtime;
	time(&curtime);
	tm *nowtime = localtime(&curtime);
	Date curr_date(nowtime->tm_year + 1900, nowtime->tm_mon + 1, nowtime->tm_mday);
	for(Date date = Date(2007, 1, 2) ; date <= curr_date; date++) {

			uint64_t key = date.DateToUint64();

			if ( !index->index_exist(key, 0) ){
					continue;
				}

			auto start1 = std::chrono::high_resolution_clock::now();
				vector<itemid_t *> items = index_read(index, key, 0);
				for (auto item : items) {
					item_list.push_back(item);
				}
		
	}

	for (auto const &local_item : item_list)
	{
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		// cout << "address = " << &r_lt_local->data << endl;
		q1_add_answer(r_lt_local, ans);
	}

	for(int i = 0; i < ans.d_size; i++) {
		if(ans.cnt[i] == 0) {
			continue;
		}
		ans.avg_amount[i] = ans.sum_amount[i] / ans.cnt[i];
		ans.avg_quantity[i] = ans.sum_quantity[i] / (double)ans.cnt[i];
	}

	auto end = std::chrono::high_resolution_clock::now();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	
	string tmp = "Q1 BWtree (ms): " + to_string(total_us/1000) + "\n";
	output_info[tid].push_back(tmp);
	
	index->UnregisterThread(tid);

	assert(rc == RCOK);
	return finish(rc);
}

RC chbench_txn_man::run_Q1_art(int tid, chbench_query * query) {

	uint64_t min_delivery_d = query->min_delivery_d;

	RC rc = RCOK;

	chbench_q1_data ans(16);
	row_t* current_row;

	long long total_us = (long long)0;

	size_t item_list_size = _wl->t_orderline->cur_tab_size * 0.5;
	itemid_t **item_list = new itemid_t*[item_list_size];

	index_art *index = _wl->i_Q1_art;

	shared_lock<shared_mutex> r_lock(index->rw_lock);
	auto start = std::chrono::high_resolution_clock::now();
	
	time_t curtime;
	time(&curtime);
	tm *nowtime = localtime(&curtime);
	Date curr_date(nowtime->tm_year + 1900, nowtime->tm_mon + 1, nowtime->tm_mday);
	Date l_date = Date(2007, 1, 2);

	size_t itemsfound = 0;
	index->lookup_range(l_date.DateToUint64(), curr_date.DateToUint64() + 10, 0, item_list, item_list_size, itemsfound);
	
	size_t item_list_used = itemsfound;
	for (size_t i = 0; i < itemsfound; i++)
	{	
		itemid_t *local_item = item_list[i];
		local_item = local_item->next;
		while(local_item) {
			item_list[item_list_used++] = local_item;
			local_item = local_item->next;
		}
	}

	if(index->index_exist(curr_date.DateToUint64(), 0)) {
		itemid_t *local_item = index_read((INDEX *)index, curr_date.DateToUint64(), 0);
		while(local_item) {
			item_list[item_list_used++] = local_item;
			local_item = local_item->next;
		}
	}

	r_lock.unlock();
	auto index_end = std::chrono::high_resolution_clock::now();
	auto index_us = std::chrono::duration_cast<std::chrono::microseconds>(index_end - start).count();

	for (size_t i = 0; i < item_list_used; i++)
	{	
		itemid_t *local_item = item_list[i];
		row_t * r_lt = ((row_t *)local_item->location);
		row_t * r_lt_local = get_row(r_lt, SCAN);
		if (r_lt_local == NULL) {
			// Skip the deleted item
			// return finish(Abort);
			continue;
		}
		// cout << "address = " << &r_lt_local->data << endl;
		uint64_t tmp_date = 0;
		r_lt_local->get_value(OL_DELIVERY_D, tmp_date);
		q1_add_answer(r_lt_local, ans);
	}

	for(int i = 0; i < ans.d_size; i++) {
		if(ans.cnt[i] == 0) {
			continue;
		}
		ans.avg_amount[i] = ans.sum_amount[i] / ans.cnt[i];
		ans.avg_quantity[i] = ans.sum_quantity[i] / (double)ans.cnt[i];
	}

	auto end = std::chrono::high_resolution_clock::now();
	total_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

	string tmp = "Q1 ART (ms): " + to_string(total_us/1000) + " index_read(ms): " + to_string(index_us/1000) + " nums: " + to_string(ans.cnt[1]) + "\n";
	output_info[tid].push_back(tmp);

	assert(rc == RCOK);
	return finish(rc);
}

RC chbench_txn_man::validate_table(int tid,chbench_query * query){
	RC rc = RCOK;
	uint64_t start_row = 0;
	uint64_t order_row_size = (uint64_t)_wl->t_order->cur_tab_size;
	uint64_t orderline_row_size = (uint64_t)_wl->t_orderline->cur_tab_size;
	uint64_t end_row = 100;
	row_t *current_row;
	row_t *order_row;
	for(uint64_t i = start_row; i < end_row; i++) {
		uint64_t order_row_id = URand(start_row, order_row_size, 0);
		// cout<<order_row_id<<endl;
		order_row = (row_t *)&(_wl->t_order->row_buffer[order_row_id]);
		assert(order_row != NULL);
		row_t *order_local = get_row(order_row, SCAN);
		if (order_row == NULL)
		{
			continue;
		}
		UInt32 o_id;
		uint64_t o_w_id;
		uint64_t o_d_id;
		order_local->get_value(O_ID, o_id);
		order_local->get_value(O_W_ID, o_w_id);
		order_local->get_value(O_D_ID, o_d_id);
		// cout << o_id << "---" << o_w_id << "---" << o_d_id << "---" << endl;
		uint64_t row_id;
		for (uint64_t j = start_row; i < orderline_row_size; j++) {
			current_row = (row_t *)&(_wl->t_orderline->row_buffer[j]);
			assert(current_row != NULL);
			row_t *row_local = get_row(current_row, SCAN);
			if (row_local == NULL)
			{
				continue;
			}
			UInt32 ol_o_id;
			row_local->get_value(OL_O_ID, ol_o_id);
			if (ol_o_id == o_id)
			{
				row_id = j;
				break;
			}
		}
		uint64_t o_ol_cnt;
		order_local->get_value(O_OL_CNT, o_ol_cnt);
		for (uint32_t ol = 1; ol <= o_ol_cnt; ol++) {
			current_row = (row_t *)&(_wl->t_orderline->row_buffer[row_id]);
			assert(current_row != NULL);
			row_t *row_local = get_row(current_row, SCAN);
			if (row_local == NULL)
			{
				continue;
			}
			UInt32 ol_o_id;
			uint64_t ol_w_id;
			uint64_t ol_d_id;
			uint64_t ol_i_id;
			uint64_t ol_number;
			double ol_amount;
			row_local->get_value(OL_O_ID, ol_o_id);
			row_local->get_value(OL_W_ID, ol_w_id);
			row_local->get_value(OL_D_ID, ol_d_id);
			row_local->get_value(OL_I_ID, ol_i_id);
			row_local->get_value(OL_NUMBER, ol_number);
			// cout << ol_o_id << "---" << ol_w_id << "---" << ol_d_id << "---" << ol_number << endl;
			assert(o_id == ol_o_id);
		}
	}
	cout << "Table order and table orderline are both correct." << endl;
	assert(rc == RCOK);
	return finish(rc);
}