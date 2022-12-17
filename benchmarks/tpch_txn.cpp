#include "tpch.h"
#include "tpch_query.h"
#include "tpc_helper.h"
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

RC tpch_txn_man::run_txn(base_query * query) 
{
	RC rc = RCOK;
	tpch_query * m_query = (tpch_query *) query;

	run_Q6_bitmap(m_query);
	run_Q6(m_query);
	run_Q6_index(m_query);

	// switch (m_query->type) {
	// 	case TPCH_Q6 :
	// 		return run_Q6(m_query); break;
	// 	case TPCH_Q6_index :
	// 		return run_Q6_index(m_query); break;
	// 	case TPCH_RF1 :
	// 		return run_RF1(); break;
	// 	case TPCH_RF2 :
	// 		return run_RF2(); break;			
	// 	default:
	// 		assert(false);
	// }

	return rc;
}

RC tpch_txn_man::run_Q6_bitmap(tpch_query *query)
{
	RC rc = RCOK;
	ibis::bitvector result{};
	nbub::Nbub *bitmap_sd, *bitmap_dc, *bitmap_qt;
	
	int year_val = (query->date / 1000) - 92;  // [0, 7]
	assert(year_val >= 1);
	assert(year_val <= 7);
	int discount_val = query->discount * 100; // [0, 10]
	assert(discount_val >= 2);
	assert(discount_val <= 9);
	int quantity_val = query->quantity - 1; // [0, 49]
	assert(quantity_val >= 0);
	assert(quantity_val <= 49);

	bitmap_sd = dynamic_cast<nbub::Nbub *>(_wl->bitmap_shipdate);
	ibis::bitvector *btv_shipdate = bitmap_sd->bitmaps[year_val]->btv;

	bitmap_dc = dynamic_cast<nbub::Nbub *>(_wl->bitmap_discount);
	ibis::bitvector btv_discount;
	btv_discount.copy(*bitmap_dc->bitmaps[discount_val-1]->btv);
	btv_discount |= *bitmap_dc->bitmaps[discount_val]->btv;
	btv_discount |= *bitmap_dc->bitmaps[discount_val+1]->btv;

	bitmap_qt = dynamic_cast<nbub::Nbub *>(_wl->bitmap_quantity);
	ibis::bitvector btv_quantity;
	btv_quantity.copy(*bitmap_qt->bitmaps[0]->btv);
	for (int i = 1; i < quantity_val; i++) {
		btv_quantity |= *bitmap_qt->bitmaps[i]->btv;
	}

	result.copy(*btv_shipdate);
	result &= btv_discount;
	result &= btv_quantity;

	// result.decompress();
	int cnt = 0;
	for (uint64_t pos = 0; pos < bitmap_sd->g_number_of_rows ; pos++) {
		if (result.getBit(pos, bitmap_sd->config)) {
			cnt ++;
		}
	}
	cout << "[run_Q6_bitmap : after merging] sizeof(result btv): " << cnt << endl; 

	return rc;
}

RC tpch_txn_man::run_Q6(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	int cnt = 0;
	INDEX * index = _wl->i_lineitem;
	
	// txn
	double revenue = 0;
	uint64_t max_items = (uint64_t) (tpch_lineitemKey(g_num_orders, 8));
	for (uint64_t i = 1; i <= max_items ; ++i) {
		// cout << "i = " << i << endl;
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
		
		// debug
		// cout << "A query_date " << query->date << " " << l_shipdate << endl;
		// cout << "A query_discount " << query->discount << " " << l_discount << endl;
		// cout << "A query_quantity " << query->quantity << " " << l_quantity << endl << endl;		

		if ((l_shipdate / 1000) == (query->date / 1000)
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

				cnt ++;
		}
	}
	cout << "********Q6            revenue is : " << revenue << "  . Number of items: " << cnt << endl; 

	assert( rc == RCOK );
	return finish(rc);
}

RC tpch_txn_man::run_Q6_index(tpch_query * query) {
	RC rc = RCOK;
	itemid_t * item;
	int cnt = 0;
	uint64_t key;
	double revenue = 0;
	INDEX * index = _wl->i_Q6_index;
	uint64_t date = query->date;	// e.g., 1st Jan. 97
	uint64_t discount = (uint64_t)(query->discount * 100); // +1 -1
	double quantity = query->quantity;
	for (uint64_t i = date; i <= (uint64_t)(date + 364); i++) {
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

					cnt ++;
					local_item = local_item->next;
				}

			}
		}
	}
	cout << "********Q6 with index revenue is : " << revenue << "  . Number of items: " << cnt << endl << endl; 
	assert( rc == RCOK );
	return finish(rc);
}

RC tpch_txn_man::run_RF1() {
	for (uint64_t i = (uint64_t)(g_num_orders + 1); i < (uint64_t)(SF * 1500 + g_num_orders + 1); ++i) {
		row_t * row;
		uint64_t row_id;
		_wl->t_orders->get_new_row(row, 0, row_id);		
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
		row->set_value(O_TOTALPRICE, (double)12345.56); // May be used
		char temp[20];
		MakeAlphaString(10, 19, temp, 0);
		row->set_value(O_ORDERPRIORITY, temp);
		row->set_value(O_CLERK, temp);
		row->set_value(O_SHIPPRIORITY, (uint64_t)654321);
		row->set_value(O_COMMENT, temp);

		// index_insert(_wl->i_orders, i, row, 0);
		_wl->index_insert(_wl->i_orders, i, row, 0);




		// **********************Lineitems*****************************************

		uint64_t lines = URand(1, 7, 0);
		// uint64_t lines = 1;
		g_total_line_in_lineitems += lines;
		for (uint64_t lcnt = 1; lcnt <= lines; lcnt++) {
			row_t * row2;
			uint64_t row_id2;
			_wl->t_lineitem->get_new_row(row2, 0, row_id2);

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
			_wl->index_insert(_wl->i_lineitem, key, row2, 0);


			// Q6 index
			uint64_t Q6_key = tpch_lineitemKey_index(shipdate, discount, (uint64_t)quantity);
			// cout << "Q6_insert_key = " << Q6_key << endl; 
			_wl->index_insert(_wl->i_Q6_index, Q6_key, row2, 0);
		}
	}
	return RCOK;
}

RC tpch_txn_man::run_RF2() {
	for (uint64_t i = 1; i < (uint64_t)(SF * 1500); ++i) {
		uint64_t key = URand(1, g_num_orders, 0);
		_wl->i_orders->index_remove(key);
		for (uint64_t lcnt = (uint64_t)1; lcnt <= (uint64_t)7; ++lcnt){
			_wl->i_lineitem->index_remove(tpch_lineitemKey(key, lcnt));
		}
	}

	return RCOK;

}