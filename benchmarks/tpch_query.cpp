#include "query.h"
#include "tpch_query.h"
#include "tpch.h"
#include "tpc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

void tpch_query::init(uint64_t thd_id, workload * h_wl) 
{
	// gen_Q6(thd_id);
	gen_Q6_ht(thd_id);
	// gen_RF1(thd_id);
	// gen_RF2(thd_id);
}

void tpch_query::gen_Q6(uint64_t thd_id) {
	type = TPCH_Q6;

	//Q6 related vars
	uint64_t year = URand(93, 97, 0);
	date = (uint64_t)(year * 1000 + 1);
	discount = ((double)URand(2, 9, 0)) / 100;
	quantity = (double)URand(24, 25, 0);

}

void tpch_query::gen_Q6_ht(uint64_t thd_id) {
	type = TPCH_Q6_HT;

	//Q6 related vars
	uint64_t year = URand(93, 97, 0);
	date = (uint64_t)(year * 1000 + 1);
	discount = ((double)URand(2, 9, 0)) / 100;
	quantity = (double)URand(24, 25, 0);
}

void tpch_query::gen_RF1(uint64_t thd_id) {
	type = TPCH_RF1;
}

void tpch_query::gen_RF2(uint64_t thd_id) {
	type = TPCH_RF2;
}