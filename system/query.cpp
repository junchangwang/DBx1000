#include <sched.h>
#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "chbench_query.h"
#include "tpc_helper.h"
#include "tpch_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/
int Query_queue::_next_tid;

void 
Query_queue::init(workload * h_wl) {
	all_queries = new Query_thd * [g_thread_cnt];
	_wl = h_wl;
	_next_tid = 0;
	

#if WORKLOAD == YCSB	
	ycsb_query::calculateDenom();
#elif WORKLOAD == TPCC
	assert(tpc_buffer != NULL);
#elif WORKLOAD == TPCH
	assert(tpc_buffer != NULL);
#elif WORKLOAD == CHBench
	assert(tpc_buffer != NULL);
#endif
	int64_t begin = get_server_clock();
	pthread_t p_thds[g_thread_cnt - 1];
	for (UInt32 i = 0; i < g_thread_cnt - 1; i++) {
		pthread_create(&p_thds[i], NULL, threadInitQuery, this);
	}
	threadInitQuery(this);
	for (uint32_t i = 0; i < g_thread_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	int64_t end = get_server_clock();
	printf("Query Queue Init Time %f\n", 1.0 * (end - begin) / 1000000000UL);
}

void 
Query_queue::init_per_thread(int thread_id) {	
	all_queries[thread_id] = (Query_thd *) _mm_malloc(sizeof(Query_thd), 64);
	all_queries[thread_id]->init(_wl, thread_id);
}

base_query * 
Query_queue::get_next_query(uint64_t thd_id) { 	
	base_query * query = all_queries[thd_id]->get_next_query();
	return query;
}

void *
Query_queue::threadInitQuery(void * This) {
	Query_queue * query_queue = (Query_queue *)This;
	uint32_t tid = ATOM_FETCH_ADD(_next_tid, 1);
	
	// set cpu affinity
	// set_affinity(tid);

	query_queue->init_per_thread(tid);
	return NULL;
}

/*************************************************/
//     class Query_thd
/*************************************************/

void 
Query_thd::init(workload * h_wl, int thread_id) {
	uint64_t request_cnt;
	uint64_t year = 0;
	uint64_t date;
	double discount;
	double quantity;
	q_idx = 0;
	request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if ABORT_BUFFER_ENABLE
    request_cnt += ABORT_BUFFER_SIZE;
#endif
#if WORKLOAD == YCSB	
	queries = (ycsb_query *) 
		mem_allocator.alloc(sizeof(ycsb_query) * request_cnt, thread_id);
	srand48_r(thread_id + 1, &buffer);
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) _mm_malloc(sizeof(tpcc_query) * request_cnt, 64);
#elif WORKLOAD == TPCH
	queries = (tpch_query *) _mm_malloc(sizeof(tpch_query) * request_cnt, 64);
#elif WORKLOAD == CHBench
	queries = (chbench_query*) _mm_malloc(sizeof(chbench_query) * request_cnt, 64);
#endif
	for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) ycsb_query();
		queries[qid].init(thread_id, h_wl, this);
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
		queries[qid].init(thread_id, h_wl);
#elif WORKLOAD == CHBench
		new(&queries[qid]) chbench_query();
		queries[qid].init(thread_id, h_wl);
#elif WORKLOAD == TPCH
		new(&queries[qid]) tpch_query();
		queries[qid].init(thread_id, h_wl);

		// Generate Q6 related vars
		// Using this way, we can guarantee each (scan, hash, btree, rabit) group use the same config.
		if (qid % 6 == 0) {
			year = URand(93, 97, 0);
			date = (uint64_t)(year * 1000 + 1);
			discount = ((double)URand(2, 9, 0)) / 100;
			quantity = (double)URand(24, 25, 0);
		}

		// Generate type
		TPCHTxnType type;
		if (qid % 6 == 0) type = TPCH_Q6_SCAN;
		else if (qid % 6 == 1) type = TPCH_Q6_HASH;
		else if (qid % 6 == 2) type = TPCH_Q6_BTREE;
		else if (qid % 6 == 3) type = TPCH_Q6_BWTREE;
		else if (qid % 6 == 4) type = TPCH_Q6_ART;
		else type = TPCH_Q6_RABIT;

		if (qid % 100 == 20) /* magic number */
			type = TPCH_RF1;
		else if (qid % 100 == 40)
			type = TPCH_RF2;

		queries[qid].type = type;

		// Assign Q6 parameters
		queries[qid].date = date;
		queries[qid].discount = discount;
		queries[qid].quantity = quantity;
		queries[qid].rf_txn_num = 0;
#endif
	}
}

base_query * 
Query_thd::get_next_query() {
#if WORKLOAD == TPCH
	if(q_idx > 0) {
		tpch_query * h_query = &queries[q_idx - 1];
		if(h_query->type == TPCH_RF1 || h_query->type == TPCH_RF2) {
			if(h_query->rf_txn_num == RF_TXN_NUM) {
				base_query * rf_query = &queries[q_idx++];
				return rf_query;
			}
			else{
				base_query * rf_query = &queries[q_idx - 1];
				return rf_query;
			}
		}
	}
#endif
	base_query * query = &queries[q_idx++];
	return query;
}
