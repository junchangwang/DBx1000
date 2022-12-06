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
			run_Q6(m_query); 
			return run_Q6_index(m_query); break;
			// return run_Q6(m_query); break;
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
	// g_total_line_in_lineitems = 10000;	// To be fixed
	uint64_t max_number = (uint64_t) (tpch_lineitemKey(g_max_lineitem+1, (uint64_t)8));
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
//b 66 if l_shipdate == (uint64_t)95129 && (int)(l_quantity) == 20 && (int)(l_discount*100) == 3 
		// begin
		uint64_t l_shipdate;
		r_lt_local->get_value(L_SHIPDATE, l_shipdate);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		double l_quantity;
		r_lt_local->get_value(L_QUANTITY, l_quantity);
		
		// debug
		// cout << "A query_date " << query->date << " " << l_shipdate << endl;
		// cout << "A query_discount " << query->discount << " " << l_discount << endl;
		// cout << "A query_quantity " << query->quantity << " " << l_quantity << endl << endl;		

		if (l_shipdate >= query->date 
			&& l_shipdate < (uint64_t)(query->date + 1000) 
			&& (uint64_t)(l_discount*100) >= (uint64_t)((uint64_t)(query->discount*100) - 1) 
			&& (uint64_t)(l_discount*100) <= (uint64_t)((uint64_t)(query->discount*100) + 1) 
			&& (uint64_t)l_quantity < (uint64_t)query->quantity){
				
				// debug
				// cout << "B query_date " << query->date << " " << l_shipdate << endl;
				// cout << "B query_discount " << query->discount << " " << l_discount << endl;
				// cout << "B query_quantity " << query->quantity << " " << l_quantity << endl << endl;
				
				// cout << "address = " << &r_lt_local->data << endl;

				double l_extendedprice;
				r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
				revenue += l_extendedprice * l_discount;
			}
	}
	cout << endl << "********Q6            revenue is *********" << revenue << endl; 

	assert( rc == RCOK );
	return finish(rc);
}

RC tpch_txn_man::run_Q6_index(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	uint64_t key;
	double revenue = 0;
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
			for (uint64_t k = (uint64_t)((uint64_t)quantity - 1); k > (uint64_t)0; k--){
	
				// key = (uint64_t)((i * 12 + j) * 26 + k);
				key = tpch_lineitemKey_index(i, j, k);
				// cout << "Q6_txn_key = " << key << endl; 
				if ( !index->index_exist(key, 0) ){
					// cout << i << " NOT EXIST!" << endl;
					continue;
				}	

				// debug
				// cout << "Q6_txn_key = " << key << endl;
				// cout << "index query_date " << query->date << " " << i << endl;
				// cout << "index query_discount " << query->discount << " " << j << endl;
				// cout << "index query_quantity " << query->quantity << " " << k << endl << endl;
				
				item = index_read(index, key, 0);
				itemid_t * local_item = item;
				assert(item != NULL);
				while (local_item != NULL) {
					row_t * r_lt = ((row_t *)local_item->location);
					row_t * r_lt_local = get_row(r_lt, RD);
					if (r_lt_local == NULL) {
						return finish(Abort);
					}
					// cout << "address = " << &r_lt_local->data << endl;
					double l_extendedprice;
					r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
					revenue += l_extendedprice * ((double)j / 100);

					local_item = local_item->next;
				}

			}
		}
	}

	// year2
	uint64_t year2 = (uint64_t)((year + 1)*1000 + 1);
	for (uint64_t i = year2; i < (uint64_t)(year2 + day - 1); i++) {
		for (uint64_t j = (uint64_t)(discount - 1); j <= (uint64_t)(discount + 1); j++) {
			for (uint64_t k = (uint64_t)quantity - 1; k > (uint64_t)0; k--){
				// key = (uint64_t)((i * 12 + j) * 26 + k);
				key = tpch_lineitemKey_index(i, j, k);
				// cout << "Q6_txn_key = " << key << endl; 
				if ( !index->index_exist(key, 0) ){
					// cout << i << " NOT EXIST!" << endl;
					continue;
				}	
				// debug
				// cout << "Q6_txn_key = " << key << endl;
				// cout << "index query_date " << query->date << " " << i << endl;
				// cout << "index query_discount " << query->discount << " " << j << endl;
				// cout << "index query_quantity " << query->quantity << " " << k << endl << endl;	

				item = index_read(index, key, 0);
				itemid_t * local_item = item;
				assert(item != NULL);
				while (local_item != NULL) {
					row_t * r_lt = ((row_t *)local_item->location);
					row_t * r_lt_local = get_row(r_lt, RD);
					if (r_lt_local == NULL) {
						return finish(Abort);
					}
					// cout << "address = " << &r_lt_local->data << endl;
					double l_extendedprice;
					r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
					revenue += l_extendedprice * ((double)j / 100);

					local_item = local_item->next;
				}
				
				// // debug
				// uint64_t l_shipdate;
				// r_lt_local->get_value(L_SHIPDATE, l_shipdate);
				// double l_discount;
				// r_lt_local->get_value(L_DISCOUNT, l_discount);
				// double l_quantity;
				// r_lt_local->get_value(L_QUANTITY, l_quantity);
				// cout << "real row query_date " << query->date << " " << l_shipdate << endl;
				// cout << "real row query_discount " << query->discount << " " << l_discount << endl;
				// cout << "real row query_quantity " << query->quantity << " " << l_quantity << endl << endl;	
				// cout << "real key" << tpch_lineitemKey_index(l_shipdate, (uint64_t)(l_discount*100), (uint64_t) quantity); 
			}
		}
	}
	cout << endl << "********Q6 with index revenue is *********" << revenue << endl << endl;
	assert( rc == RCOK );
	return finish(rc);
}