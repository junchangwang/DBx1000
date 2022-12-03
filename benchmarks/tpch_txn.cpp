#include "tpch.h"
#include "tpch_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpch_const.h"

void tpch_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpch_wl *) h_wl;
}

RC tpch_txn_man::run_txn(base_query * query) {
	tpch_query * m_query = (tpch_query *) query;
	switch (m_query->type) {
		case TPCH_Q6 :
			//return run_Q6(m_query); break;
			return run_Q6_index(m_query); break;
		default:
			assert(false);
	}
}

RC tpch_txn_man::run_Q6(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	INDEX * index = _wl->i_lineitem;
	
	// txn
	double revenue = 0;
	// uint64_t max_item = 10000;
	g_total_line_in_lineitems = 10000;	// To be fixed
	uint64_t max_number = 7*10000;
	for (uint64_t i = 1; i <= max_number; ++i) {
		// cout << endl << "iiiii = " << i << endl;
		if ( !index->index_exist(i, 0) ){
			// cout << i << " NOT EXIST!" << endl;
			continue;
		}
		item = index_read(index, i, 0);
		assert(item != NULL);
		row_t * r_lt = ((row_t *)item->location);
		row_t * r_lt_local = get_row(r_lt, RD);
		if (r_lt_local == NULL) {
			return finish(Abort);
		}

		// begin
		uint64_t l_shipdate;
		r_lt_local->get_value(L_SHIPDATE, l_shipdate);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		double l_quantity;
		r_lt_local->get_value(L_QUANTITY, l_quantity);

		if (l_shipdate >= query->date 
			&& l_shipdate < (uint64_t)(query->date + 100) 
			&& l_discount >= query->discount - 0.01 
			&& l_discount <= query->discount + 0.01 
			&& l_quantity < query->quantity){
				double l_extendedprice;
				r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
				revenue += l_extendedprice * l_discount;
			}
	}
	cout << endl << "********revenue is *********" << revenue << endl; 

	assert( rc == RCOK );
	return finish(rc);
}

RC tpch_txn_man::run_Q6_index(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	uint64_t key;
	double revenue;
	INDEX * index = _wl->i_Q6_index;
	uint64_t date = query->date;	// + 1 year
	uint64_t discount = (uint64_t)(query->discount * 100); // +1 -1
	double quantity = query->quantity;
	uint64_t year = date / 1000;
	uint64_t day = date % 1000;
	uint64_t diff = 365 - day;
	uint64_t year1 = date;
	for (uint64_t i = year1; i <= year1 + diff; i++) {
		for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
			key = (uint64_t)((i * 12 + j) * 26 + (uint64_t)quantity);
			if ( !index->index_exist(key, 0) ){
				// cout << i << " NOT EXIST!" << endl;
				continue;
			}	
			item = index_read(index, key, 0);
			assert(item != NULL);
			row_t * r_lt = ((row_t *)item->location);
			row_t * r_lt_local = get_row(r_lt, RD);
			if (r_lt_local == NULL) {
				return finish(Abort);
			}
			double l_extendedprice;
			r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
			revenue += l_extendedprice * discount;
		}
	}

	// year2
	uint64_t year2 = (uint64_t)((year + 1)*1000 + 1);
	for (uint64_t i = year2; i <= year2 + day; i++) {
		for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
			key = (uint64_t)((i * 12 + j) * 26 + (uint64_t)quantity);
			if ( !index->index_exist(key, 0) ){
				// cout << i << " NOT EXIST!" << endl;
				continue;
			}	
			item = index_read(index, key, 0);
			assert(item != NULL);
			row_t * r_lt = ((row_t *)item->location);
			row_t * r_lt_local = get_row(r_lt, RD);
			if (r_lt_local == NULL) {
				return finish(Abort);
			}
			double l_extendedprice;
			r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
			revenue += l_extendedprice * discount;
		}
	}
	cout << endl << "********revenue is *********" << revenue << endl;
	assert( rc == RCOK );
	return finish(rc);
}