#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_ptmvcc.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == PTMVCC

void Row_ptmvcc::init(row_t * row) {
	_his_len = 4;

	_write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
	for (uint32_t i = 0; i < _his_len; i++) 
		_write_history[i].row = NULL;
	_write_history[0].row = row;
	_write_history[0].begin_txn = false;
	_write_history[0].end_txn = false;
	_write_history[0].begin = 0;
	_write_history[0].end = INF;

	_his_latest = 0;
	_his_oldest = 0;
	_exists_prewrite = false;

	blatch = false;
}

void 
Row_ptmvcc::doubleHistory()
{
	WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len * 2, 64);
	uint32_t idx = _his_oldest; 
	for (uint32_t i = 0; i < _his_len; i++) {
		temp[i] = _write_history[idx]; 
		idx = (idx + 1) % _his_len;
		temp[i + _his_len].row = NULL;
		temp[i + _his_len].begin_txn = false;
		temp[i + _his_len].end_txn = false;
	}

	_his_oldest = 0;
	_his_latest = _his_len - 1; 
	_mm_free(_write_history);
	_write_history = temp;

	_his_len *= 2;
}

RC Row_ptmvcc::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	ts_t ts = txn->get_ts();
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
	if (!_write_history[_his_latest].end_txn && _write_history[_his_latest].end != INF) {
		// cout << "=========== The tuple has been deleted ================" << endl;
		rc = Abort;
		goto out;
	}
	assert(_write_history[_his_latest].end == INF || _write_history[_his_latest].end_txn);

	if (type == R_REQ) {
		if (ts < _write_history[_his_oldest].begin) { 
			rc = Abort;
		} else if (ts > _write_history[_his_latest].begin) {
			if (_write_history[_his_latest].end_txn == false) {
				txn->cur_row = _write_history[_his_latest].row;
				rc = RCOK;
			}
			else {
				uint32_t _his_next = (_his_latest + 1) % _his_len;
				txn->cur_row = _write_history[_his_latest].row;
				// TODO. If the corresponding txn has committed, we can return speculative entries.
				// For now, I always assume that it has committed.
				// if (global_txns[_write_history[_next_his].begin_txn].committed_ts <= txn->get_ts())
				uint32_t tid = _write_history[_his_latest].end;
				for (UInt32 i = 0; i < g_thread_cnt; i++) { 
					txn_man * txn_t = glob_manager->_all_txns[i];
					if (txn_t->get_txn_id() == tid)
					{
						if (txn_t->committed_ts <= txn->get_ts()) {
							txn->cur_row = _write_history[_his_next].row;
							break;
						}
					}
				}
				rc = RCOK;
			}
		} else {
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t i = _his_latest;
			bool find = false;
			while (true) {
				i = (i == 0)? _his_len - 1 : i - 1;
				if (_write_history[i].begin < ts) {
					assert(_write_history[i].end > ts);
					txn->cur_row = _write_history[i].row;
					find = true;
					break;
				}
				if (i == _his_oldest)
					break;
			}
			assert(find);
		}
	} else if (type == P_REQ) {
		if (_exists_prewrite || ts < _write_history[_his_latest].begin) {
			rc = Abort;
		} else {
			rc = RCOK;
			_exists_prewrite = true;
			uint32_t id = reserveRow(txn);
			uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
			_write_history[id].begin_txn = true;
			_write_history[id].begin = txn->get_txn_id();
			_write_history[pre_id].end_txn = true;
			_write_history[pre_id].end = txn->get_txn_id();
			row_t * res_row = _write_history[id].row;
			assert(res_row);
			res_row->copy(_write_history[_his_latest].row);
			txn->cur_row = res_row;
		}
	} else if (type == D_REQ) {
		if (_exists_prewrite || ts < _write_history[_his_latest].begin) {
			rc = Abort;
		} else {
			rc = RCOK;
			_exists_prewrite = true;
			_write_history[_his_latest].end_txn = true;
			_write_history[_his_latest].end = txn->get_txn_id();
			txn->cur_row = _write_history[_his_latest].row;
		}
	} else 
		assert(false);

out:
	blatch = false;
	return rc;
}

uint32_t 
Row_ptmvcc::reserveRow(txn_man * txn)
{
	// Garbage Collection
	uint32_t idx;
	ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id());
	if ((_his_latest + 1) % _his_len == _his_oldest // history is full
		&& min_ts > _write_history[_his_oldest].end) 
	{
		while (_write_history[_his_oldest].end < min_ts) 
		{
			assert(_his_oldest != _his_latest);
			_his_oldest = (_his_oldest + 1) % _his_len;
		}
	}
	
	if ((_his_latest + 1) % _his_len != _his_oldest) 
		// _write_history is not full, return the next entry.
		idx = (_his_latest + 1) % _his_len;
	else { 
		// write_history is already full
		// If _his_len is small, double it. 
		if (_his_len < g_thread_cnt) {
			doubleHistory();
			idx = (_his_latest + 1) % _his_len;
		} else {
			// _his_len is too large, should replace the oldest history
			idx = _his_oldest;
			_his_oldest = (_his_oldest + 1) % _his_len;
		}
	}

	// some entries are not taken. But the row of that entry is NULL.
	if (!_write_history[idx].row) {
		_write_history[idx].row = (row_t *) _mm_malloc(sizeof(row_t), 64);
		_write_history[idx].row->init(MAX_TUPLE_SIZE);
	}
	return idx;
}

void
Row_ptmvcc::post_process(txn_man * txn, ts_t commit_ts, access_t type, RC rc)
{
	while (!ATOM_CAS(blatch, false, true))
		PAUSE

	if (type == WR) {
		WriteHisEntry * entry = &_write_history[ (_his_latest + 1) % _his_len ];
		assert(entry->begin_txn && entry->begin == txn->get_txn_id());
		_write_history[ _his_latest ].end_txn = false;
		_exists_prewrite = false;
		if (rc == RCOK) {
			assert(commit_ts > _write_history[_his_latest].begin);
			_write_history[ _his_latest ].end = commit_ts;
			entry->begin = commit_ts;
			entry->end = INF;
			_his_latest = (_his_latest + 1) % _his_len;
			assert(_his_latest != _his_oldest);
		} else 
			_write_history[ _his_latest ].end = INF;
	}
	else if (type == DEL) {
		WriteHisEntry * entry = &_write_history[_his_latest];
		assert(!entry->begin_txn && entry->end_txn && entry->end == txn->get_txn_id());
		entry->end_txn = false;
		_exists_prewrite = false;
		if (rc == RCOK) {
			assert(commit_ts > entry->begin);
			entry->end = commit_ts;
		} else
			entry->end = INF;
	}
	else
		assert(false);
	
	blatch = false;
}

#endif
