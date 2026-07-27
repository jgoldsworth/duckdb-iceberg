#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

#include "core/deletes/iceberg_equality_delete.hpp"

namespace duckdb {

//! Anti-join of the privately scanned equality-delete columns against the delete tuples that apply to
//! one data file. Built once per data file, probed once per chunk.
class IcebergEqualityDeleteFilter {
public:
	//! 'id_to_local_column' decides which fields the data file actually has, 'field_id_to_probe_index' and
	//! 'probe_types' describe the chunk that Select() is given. Returns nullptr when no tuple can match.
	static unique_ptr<IcebergEqualityDeleteFilter>
	Create(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
	       const unordered_map<int32_t, vector<idx_t>> &id_to_local_column,
	       const unordered_map<int32_t, idx_t> &field_id_to_probe_index, const vector<LogicalType> &probe_types);

	//! Writes the surviving rows into 'sel' and returns how many there are
	idx_t Select(DataChunk &probe_chunk, SelectionVector &sel) const;

private:
	//! The delete tuples that share one set of key columns
	struct KeyGroup {
		//! Position of each key column in the probe chunk, ascending
		vector<idx_t> probe_indexes;
		vector<LogicalType> types;
		//! Delete values, [key column][tuple]
		vector<vector<Value>> values;
		idx_t count = 0;
		unordered_map<hash_t, vector<idx_t>> tuples_by_hash;

		bool Matches(DataChunk &probe_chunk, idx_t row, idx_t tuple) const;
		void BuildIndex();
	};

private:
	vector<KeyGroup> groups;
	//! Set when a delete tuple matches every row of the data file
	bool deletes_everything = false;
};

} // namespace duckdb
