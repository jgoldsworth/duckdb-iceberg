#include "core/deletes/iceberg_equality_delete_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>

namespace duckdb {

namespace {

//! A delete value can only match rows of the scanned column type, so a value that does not survive the
//! round trip through that type cannot delete anything.
bool TryCastDeleteValue(const Value &input, const LogicalType &type, Value &result) {
	if (input.type() == type) {
		result = input;
		return true;
	}
	if (input.IsNull()) {
		result = Value(type);
		return true;
	}
	Value casted;
	if (!input.DefaultTryCastAs(type, casted, nullptr, true)) {
		return false;
	}
	Value round_tripped;
	if (!casted.DefaultTryCastAs(input.type(), round_tripped, nullptr, true)) {
		return false;
	}
	if (!ValueOperations::NotDistinctFrom(round_tripped, input)) {
		return false;
	}
	result = std::move(casted);
	return true;
}

} // namespace

bool IcebergEqualityDeleteFilter::KeyGroup::Matches(DataChunk &probe_chunk, idx_t row, idx_t tuple) const {
	for (idx_t key_idx = 0; key_idx < probe_indexes.size(); key_idx++) {
		auto value = probe_chunk.data[probe_indexes[key_idx]].GetValue(row);
		if (!ValueOperations::NotDistinctFrom(value, values[key_idx][tuple])) {
			return false;
		}
	}
	return true;
}

void IcebergEqualityDeleteFilter::KeyGroup::BuildIndex() {
	Vector hashes(LogicalType::HASH, STANDARD_VECTOR_SIZE);
	vector<Vector> key_vectors;
	key_vectors.reserve(types.size());
	for (auto &type : types) {
		key_vectors.emplace_back(type, STANDARD_VECTOR_SIZE);
	}

	for (idx_t offset = 0; offset < count; offset += STANDARD_VECTOR_SIZE) {
		auto batch = MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - offset);
		for (idx_t key_idx = 0; key_idx < key_vectors.size(); key_idx++) {
			auto &key_vector = key_vectors[key_idx];
			auto &key_values = values[key_idx];
			for (idx_t i = 0; i < batch; i++) {
				key_vector.SetValue(i, key_values[offset + i]);
			}
		}
		VectorOperations::Hash(key_vectors[0], hashes, batch);
		for (idx_t key_idx = 1; key_idx < key_vectors.size(); key_idx++) {
			VectorOperations::CombineHash(hashes, key_vectors[key_idx], batch);
		}
		UnifiedVectorFormat hash_format;
		hashes.ToUnifiedFormat(hash_format);
		auto hash_data = UnifiedVectorFormat::GetData<hash_t>(hash_format);
		for (idx_t i = 0; i < batch; i++) {
			tuples_by_hash[hash_data[hash_format.sel->get_index(i)]].push_back(offset + i);
		}
	}
}

unique_ptr<IcebergEqualityDeleteFilter>
IcebergEqualityDeleteFilter::Create(const vector<reference<const IcebergEqualityDeleteFile>> &delete_files,
                                    const unordered_map<int32_t, vector<idx_t>> &id_to_local_column,
                                    const unordered_map<int32_t, idx_t> &field_id_to_probe_index,
                                    const vector<LogicalType> &probe_types) {
	if (delete_files.empty()) {
		return nullptr;
	}

	auto result = make_uniq<IcebergEqualityDeleteFilter>();
	map<vector<idx_t>, idx_t> group_lookup;

	for (auto &delete_file_ref : delete_files) {
		auto &delete_file = delete_file_ref.get();
		if (delete_file.equality_values.empty()) {
			continue;
		}
		auto row_count = delete_file.equality_values.begin()->second.size();
		for (auto &entry : delete_file.equality_values) {
			if (entry.second.size() != row_count) {
				throw InvalidConfigurationException("Equality delete file contains columns with differing row counts");
			}
		}
		if (row_count == 0) {
			continue;
		}

		//! A field the data file does not have reads as NULL for every row, whatever its Iceberg initial
		//! default says, so it only matches delete tuples that are NULL there.
		vector<int32_t> absent_field_ids;
		//! 'equality_values' is unordered, so fix the key order to keep grouping and hashing deterministic
		vector<pair<idx_t, int32_t>> present;
		for (auto &entry : delete_file.equality_values) {
			if (!id_to_local_column.count(entry.first)) {
				absent_field_ids.push_back(entry.first);
				continue;
			}
			auto probe_entry = field_id_to_probe_index.find(entry.first);
			if (probe_entry == field_id_to_probe_index.end()) {
				throw InternalException("Missing private scan column for equality-delete field id %d", entry.first);
			}
			present.emplace_back(probe_entry->second, entry.first);
		}
		std::sort(present.begin(), present.end());

		vector<idx_t> probe_indexes;
		vector<int32_t> present_field_ids;
		for (auto &item : present) {
			probe_indexes.push_back(item.first);
			present_field_ids.push_back(item.second);
		}

		auto group_entry = group_lookup.find(probe_indexes);
		if (group_entry == group_lookup.end()) {
			group_entry = group_lookup.emplace(probe_indexes, result->groups.size()).first;
			KeyGroup group;
			group.probe_indexes = probe_indexes;
			for (auto probe_index : probe_indexes) {
				D_ASSERT(probe_index < probe_types.size());
				group.types.push_back(probe_types[probe_index]);
			}
			group.values.resize(probe_indexes.size());
			result->groups.push_back(std::move(group));
		}
		auto &group = result->groups[group_entry->second];

		for (idx_t tuple = 0; tuple < row_count; tuple++) {
			bool matchable = true;
			for (auto field_id : absent_field_ids) {
				if (!delete_file.equality_values.at(field_id)[tuple].IsNull()) {
					matchable = false;
					break;
				}
			}
			if (!matchable) {
				continue;
			}
			vector<Value> key;
			key.reserve(present_field_ids.size());
			for (idx_t key_idx = 0; key_idx < present_field_ids.size(); key_idx++) {
				auto &input = delete_file.equality_values.at(present_field_ids[key_idx])[tuple];
				Value value;
				if (!TryCastDeleteValue(input, group.types[key_idx], value)) {
					matchable = false;
					break;
				}
				key.push_back(std::move(value));
			}
			if (!matchable) {
				continue;
			}
			if (key.empty()) {
				result->deletes_everything = true;
				continue;
			}
			for (idx_t key_idx = 0; key_idx < key.size(); key_idx++) {
				group.values[key_idx].push_back(std::move(key[key_idx]));
			}
			group.count++;
		}
	}

	idx_t total = 0;
	for (auto &group : result->groups) {
		total += group.count;
	}
	if (total == 0 && !result->deletes_everything) {
		return nullptr;
	}
	for (auto &group : result->groups) {
		if (group.count > 0) {
			group.BuildIndex();
		}
	}
	return result;
}

idx_t IcebergEqualityDeleteFilter::Select(DataChunk &probe_chunk, SelectionVector &sel) const {
	auto count = probe_chunk.size();
	if (count == 0 || deletes_everything) {
		return 0;
	}

	vector<reference<const KeyGroup>> active;
	vector<Vector> group_hashes;
	for (auto &group : groups) {
		if (group.count == 0) {
			continue;
		}
		Vector hashes(LogicalType::HASH, STANDARD_VECTOR_SIZE);
		VectorOperations::Hash(probe_chunk.data[group.probe_indexes[0]], hashes, count);
		for (idx_t key_idx = 1; key_idx < group.probe_indexes.size(); key_idx++) {
			VectorOperations::CombineHash(hashes, probe_chunk.data[group.probe_indexes[key_idx]], count);
		}
		active.emplace_back(group);
		group_hashes.push_back(std::move(hashes));
	}
	if (active.empty()) {
		return count;
	}

	vector<UnifiedVectorFormat> hash_formats(active.size());
	for (idx_t group_idx = 0; group_idx < active.size(); group_idx++) {
		group_hashes[group_idx].ToUnifiedFormat(hash_formats[group_idx]);
	}

	idx_t kept = 0;
	for (idx_t row = 0; row < count; row++) {
		bool deleted = false;
		for (idx_t group_idx = 0; group_idx < active.size() && !deleted; group_idx++) {
			auto &group = active[group_idx].get();
			auto &hash_format = hash_formats[group_idx];
			auto hash_data = UnifiedVectorFormat::GetData<hash_t>(hash_format);
			auto entry = group.tuples_by_hash.find(hash_data[hash_format.sel->get_index(row)]);
			if (entry == group.tuples_by_hash.end()) {
				continue;
			}
			for (auto tuple : entry->second) {
				if (group.Matches(probe_chunk, row, tuple)) {
					deleted = true;
					break;
				}
			}
		}
		if (!deleted) {
			sel.set_index(kept++, row);
		}
	}
	return kept;
}

} // namespace duckdb
