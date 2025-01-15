#include <mutex>

#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "tpch.h"
#include "chbench.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

#include "perf.h"
#include "nicolas/util.h"

#define WORKERS_PER_MERGE_TH (4)

void * f(void *);

thread_t ** m_thds;

extern CHBenchQuery query_number;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	parser(argc, argv);
	
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");

	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl;
			break;
		case TPCC :
			m_wl = new tpcc_wl;
			break;
		case TPCH :
			m_wl = new tpch_wl;
			break;
		case CHBench:
			m_wl = new chbench_wl;
			break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			break;
		default:
			assert(false);	
	}
	
	if (Mode == NULL || (Mode && strcmp(Mode, "cache") == 0)) {
		m_wl->init();
		printf("workload initialized!\n");
	}

	if (Mode && strcmp(Mode, "build") == 0) {
		// ((tpch_wl *)m_wl)->build();
		if(WORKLOAD == TPCH)
			dynamic_cast<tpch_wl *>(m_wl)->build();
		else if(WORKLOAD == CHBench)
			dynamic_cast<chbench_wl *>(m_wl)->build();
		return 0;
	}
	
	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt - 1];
	m_thds = new thread_t * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
	if (WORKLOAD != TEST)
		query_queue->init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i]->init(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// Intialize the background merge threads for RABIT.

#if (EVA_RABIT == true)
    int n_merge_ths;
    std::thread *merge_ths;
	std::thread *merge_ths2;
	Table_config *config = NULL;
	BaseTable *bitmap1 = NULL;
	BaseTable *bitmap2 = NULL;
	std::thread merge_thread1;
	std::thread merge_thread2;
	
	if ( WORKLOAD == TPCC ) {
		bitmap1 = dynamic_cast<tpcc_wl *>(m_wl)->bitmap_s_quantity;
		config = bitmap1->config;

		if (config->approach == "rabit") 
		{
			__atomic_store_n(&run_merge_func, true, MM_CST);
			merge_thread1 = std::thread(rabit_merge_dispatcher, bitmap1);
		}
	}
	else if (WORKLOAD == CHBench) {
		if(query_number == CHBenchQuery::CHBenchQ1) {
			bitmap1 = dynamic_cast<chbench_wl *>(m_wl)->bitmap_q1_deliverydate;
			merge_thread1 = std::thread(rabit_merge_dispatcher, bitmap1);
			bitmap2 = dynamic_cast<chbench_wl *>(m_wl)->bitmap_q1_ol_number;
			merge_thread2 = std::thread(rabit_merge_dispatcher, bitmap2);
		}
		else {
			bitmap1 = dynamic_cast<chbench_wl *>(m_wl)->bitmap_q6_deliverydate;
			merge_thread1 = std::thread(rabit_merge_dispatcher, bitmap1);
			bitmap2 = dynamic_cast<chbench_wl *>(m_wl)->bitmap_q6_quantity;
			merge_thread2 = std::thread(rabit_merge_dispatcher, bitmap2);
		}
	}
	else if (WORKLOAD == TPCH) {
		bitmap1 = dynamic_cast<tpch_wl *>(m_wl)->bitmap_discount;
		merge_thread1 = std::thread(rabit_merge_dispatcher, bitmap1);
		bitmap2 = dynamic_cast<tpch_wl *>(m_wl)->bitmap_quantity;
		merge_thread2 = std::thread(rabit_merge_dispatcher, bitmap2);
	}
#endif

	auto start = std::chrono::high_resolution_clock::now();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(thd_cnt - 1));

	for (uint32_t i = 0; i < thd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	//int64_t endtime = get_server_clock();
	auto end = std::chrono::high_resolution_clock::now();
	double time_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

// #if (WORKLOAD == TPCC && TPCC_EVA_RABIT == true) || (WORKLOAD == CHBench && CHBENCH_EVA_RABIT == true)
// 	if (config->approach == "rabit")
// 	{
// 		__atomic_store_n(&run_merge_func, false, MM_CST);

// 		for (int i = 0; i < n_merge_ths; i++) {
// 			merge_ths[i].join();
// 		}
// 		delete[] merge_ths;
// 		#if WORKLOAD == CHBench
// 		for (int i = 0; i < n_merge_ths; i++) {
// 			merge_ths2[i].join();
// 		}
// 		delete[] merge_ths2;
// 		#endif
// 	}
// #endif
	
	if (WORKLOAD != TEST) {
		printf("=== End of simulation === \nSimTime = %.2f(ms), Throughput = %.0f(txn/s)\n", time_elapsed_us/1000, (MAX_TXN_PER_PART*g_thread_cnt) / (time_elapsed_us/1000000.0));
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}
	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}
