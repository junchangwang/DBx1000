#ifndef _CHBENCH_QUERY_H_
#define _CHBENCH_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;

// items of new order transaction
struct Item_no_chbench {
	uint64_t ol_i_id;
	uint64_t ol_supply_w_id;
	uint64_t ol_quantity;
};

class chbench_query : public base_query {
public:
	void init(uint64_t thd_id, workload * h_wl);
	CHBenchTxnType type;
	/**********************************************/	
	// common txn input for both payment & new-order
	/**********************************************/	
	uint64_t w_id;
	uint64_t d_id;
	uint64_t c_id;
	/**********************************************/	
	// txn input for payment
	/**********************************************/	
	uint64_t d_w_id;
	uint64_t c_w_id;
	uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
	double h_amount;
	bool by_last_name;
	/**********************************************/	
	// txn input for new-order
	/**********************************************/
	Item_no_chbench * items;
	bool rbk;
	bool remote;
	uint64_t ol_cnt;
	uint64_t o_entry_d;
	// Input for delivery
	uint64_t o_carrier_id;
	uint64_t ol_delivery_d;
	// for order-status

	// for chbench_q6
	static int q6_id;
	static int q1_id;
	uint64_t min_delivery_d;
	uint64_t max_delivery_d;
	int64_t min_quantity;
	int64_t max_quantity;


private:
	// warehouse id to partition id mapping
//	uint64_t wh_to_part(uint64_t wid);
	void gen_payment(uint64_t thd_id);
	void gen_new_order(uint64_t thd_id);
	void gen_order_status(uint64_t thd_id);
	void gen_q6(uint64_t thd_id);
	void gen_q1(uint64_t thd_id);
};

#endif
