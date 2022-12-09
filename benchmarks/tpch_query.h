#ifndef _TPCH_QUERY_H_
#define _TPCH_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;

// items of new order transaction
// struct Item_no {
// 	uint64_t ol_i_id;
// 	uint64_t ol_supply_w_id;
// 	uint64_t ol_quantity;
// };

class tpch_query : public base_query {
public:
	void init(uint64_t thd_id, workload * h_wl);
	TPCHTxnType type;

	/**********************************************/	
	// txn input for Q6
	/**********************************************/	
	uint64_t id; // Not used in tpch query
	uint64_t date;
	double discount;
	double quantity;
	



private:
//	uint64_t wh_to_part(uint64_t wid);
	void gen_Q6(uint64_t thd_id);
	void gen_Q6_index(uint64_t thd_id);
	void gen_RF1(uint64_t thd_id);
	void gen_RF2(uint64_t thd_id);
};

#endif
