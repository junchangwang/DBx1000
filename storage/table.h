#pragma once 

#include "global.h"

// only index access is supported for table. 
class Catalog;
class row_t;

class table_t {
public:
	void init(Catalog * schema);
	// row lookup should be done with index. But index does not have
	// records for new rows. get_new_row returns the pointer to a 
	// new row.	
	RC get_new_row(row_t *& row); // this is equivalent to insert()
	RC get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id);

	void delete_row(); // TODO delete_row is not supportet yet

	uint64_t get_table_size() { return cur_tab_size; };
	Catalog * get_schema() { return schema; };
	const char * get_table_name() { return table_name; };

	Catalog * 		schema;

	// Sequential layout version
	RC init_row_buffer(int n_size);
	RC get_new_row_seq(row_t *& row, uint64_t part_id, uint64_t &row_id);
	
	row_t *row_buffer;
	int max_rows;
	uint64_t cur_tab_size;

private:
	const char * 	table_name;
	char 			pad[CL_SIZE - sizeof(void *)*3];
};
