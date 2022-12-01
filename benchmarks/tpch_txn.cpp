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
			return run_Q6(m_query); break;
		default:
			assert(false);
	}
}

RC tpch_txn_man::run_Q6(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	INDEX * index = _wl->i_lineitem;
	
	//trx d
	int64_t revenue = 0;
	uint64_t max_item = 10000;
	for (uint64_t i = 0; i < max_item; ++i) {
		//cout << endl << "iiiii = " << i << endl;
		item = index_read(index, i, 0);
		assert(item != NULL);
		row_t * r_lt = ((row_t *)item->location);
		row_t * r_lt_local = get_row(r_lt, WR);
		if (r_lt_local == NULL) {
			return finish(Abort);
		}

		//begin
		int64_t l_shipdate;
		r_lt_local->get_value(L_SHIPDATE, l_shipdate);
		double l_discount;
		r_lt_local->get_value(L_DISCOUNT, l_discount);
		double l_quantity;
		r_lt_local->get_value(L_QUANTITY, l_quantity);

		if (l_shipdate >= query->date &&
			l_shipdate <= query->date + 11){
				double l_extendedprice;
				r_lt_local->get_value(L_EXTENDEDPRICE, l_extendedprice);
				revenue += l_extendedprice * l_discount;
			}
	}
	cout << endl << "********revenue is *********" << revenue << endl; 

	assert( rc == RCOK );
	return finish(rc);
}
