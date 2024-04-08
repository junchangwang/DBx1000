#include "txn.h"
#include "row.h"
#include "row_mvrlu.h"
#include "manager.h"

#if CC_ALG==MVRLU

RC
txn_man::validate_mvrlu(RC rc)
{
	uint64_t starttime = get_sys_clock();
	INC_STATS(get_thd_id(), debug1, get_sys_clock() - starttime);
	ts_t commit_ts = glob_manager->get_ts(get_thd_id());
	// validate the read set.
#if ISOLATION_LEVEL == SERIALIZABLE
	cout << "The MVRLU mode has not supported SERIALIZABLE yet."
	assert(0);
#endif
	// postprocess 
	for (int rid = 0; rid < row_cnt; rid ++) {
		access_t type = accesses[rid]->type;
		if (type == RD)
			continue;
		accesses[rid]->orig_row->manager->post_process(this, commit_ts, type, rc);
	}
	return rc;
}

#endif
