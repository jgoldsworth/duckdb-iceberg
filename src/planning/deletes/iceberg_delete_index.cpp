#include "planning/deletes/iceberg_delete_index.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

static hash_t HashPartitionValue(const Value &value) {
	if (value.IsNull()) {
		return 0x9e3779b97f4a7c15ULL;
	}
	switch (value.type().id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return Hash<int64_t>(value.GetValue<int64_t>());
	default:
		return value.Hash();
	}
}

static hash_t HashPartitionTuple(const vector<IcebergPartitionInfo> &partition) {
	hash_t result = 0;
	for (auto &info : partition) {
		result = CombineHash(result, CombineHash(Hash<uint64_t>(info.field_id), HashPartitionValue(info.value)));
	}
	return result;
}

IcebergDeleteIndex::IcebergDeleteIndex(const IcebergTableMetadata &metadata) : metadata(metadata) {
}

optional_ptr<const vector<idx_t>>
IcebergDeleteIndex::FindManifestCandidates(idx_t data_manifest_idx, int64_t data_sequence_number,
                                           const vector<IcebergPartitionInfo> &data_partition) const {
	auto bucket = manifest_memo.find(CombineHash(Hash<idx_t>(data_manifest_idx), HashPartitionTuple(data_partition)));
	if (bucket == manifest_memo.end()) {
		return nullptr;
	}
	for (auto &memo : bucket->second) {
		if (memo.data_manifest_idx != data_manifest_idx || memo.data_sequence_number != data_sequence_number ||
		    memo.data_partition.size() != data_partition.size()) {
			continue;
		}
		bool same = true;
		for (idx_t i = 0; i < data_partition.size() && same; i++) {
			auto &left = memo.data_partition[i];
			auto &right = data_partition[i];
			same = left.field_id == right.field_id && left.value.type() == right.value.type() &&
			       Value::NotDistinctFrom(left.value, right.value);
		}
		if (same) {
			return memo.candidates;
		}
	}
	return nullptr;
}

void IcebergDeleteIndex::StoreManifestCandidates(idx_t data_manifest_idx, int64_t data_sequence_number,
                                                 const vector<IcebergPartitionInfo> &data_partition,
                                                 vector<idx_t> candidates) {
	auto key = CombineHash(Hash<idx_t>(data_manifest_idx), HashPartitionTuple(data_partition));
	manifest_memo[key].push_back({data_manifest_idx, data_sequence_number, data_partition, std::move(candidates)});
}

vector<idx_t> IcebergDeleteIndex::UnindexedManifests(const vector<idx_t> &manifest_indexes) const {
	vector<idx_t> result;
	for (auto manifest_idx : manifest_indexes) {
		if (!indexed_manifests.count(manifest_idx)) {
			result.push_back(manifest_idx);
		}
	}
	return result;
}

optional<hash_t> IcebergDeleteIndex::PartitionKey(int32_t spec_id,
                                                  const vector<IcebergPartitionInfo> &partition) const {
	auto spec_it = metadata.partition_specs.find(spec_id);
	if (spec_it == metadata.partition_specs.end()) {
		return optional<hash_t>();
	}
	hash_t result = Hash<int32_t>(spec_id);
	for (auto &field : spec_it->second.fields) {
		auto info = std::find_if(partition.begin(), partition.end(), [&](const IcebergPartitionInfo &candidate) {
			return candidate.field_id == field.partition_field_id;
		});
		if (info == partition.end()) {
			return optional<hash_t>();
		}
		result = CombineHash(result, HashPartitionValue(info->value));
	}
	return result;
}

void IcebergDeleteIndex::AddEntry(const IcebergDeleteFileReference &reference, int32_t delete_spec_id,
                                  const IcebergDataFile &delete_file) {
	if (delete_file.referenced_data_file) {
		by_referenced_data_file[*delete_file.referenced_data_file].push_back(reference);
		return;
	}
	auto spec_it = metadata.partition_specs.find(delete_spec_id);
	if (spec_it == metadata.partition_specs.end() || spec_it->second.IsUnpartitioned()) {
		unpartitioned.push_back(reference);
		return;
	}
	auto key = PartitionKey(delete_spec_id, delete_file.partition_info);
	if (key) {
		by_partition[*key].push_back(reference);
	}
}

void IcebergDeleteIndex::MarkIndexed(const vector<idx_t> &manifest_indexes) {
	indexed_manifests.insert(manifest_indexes.begin(), manifest_indexes.end());
}

vector<IcebergDeleteFileReference>
IcebergDeleteIndex::FindEntryCandidates(const vector<idx_t> &manifest_indexes, const string &data_file_path,
                                        int32_t data_spec_id,
                                        const vector<IcebergPartitionInfo> &data_partition) const {
	vector<IcebergDeleteFileReference> result;
	auto append = [&](const vector<IcebergDeleteFileReference> &bucket) {
		for (auto &reference : bucket) {
			if (std::binary_search(manifest_indexes.begin(), manifest_indexes.end(), reference.manifest_idx)) {
				result.push_back(reference);
			}
		}
	};
	auto referenced = by_referenced_data_file.find(data_file_path);
	if (referenced != by_referenced_data_file.end()) {
		append(referenced->second);
	}
	append(unpartitioned);
	auto key = PartitionKey(data_spec_id, data_partition);
	if (key) {
		auto bucket = by_partition.find(*key);
		if (bucket != by_partition.end()) {
			append(bucket->second);
		}
	}
	std::sort(result.begin(), result.end(),
	          [](const IcebergDeleteFileReference &left, const IcebergDeleteFileReference &right) {
		          return left.manifest_idx != right.manifest_idx ? left.manifest_idx < right.manifest_idx
		                                                         : left.entry_idx < right.entry_idx;
	          });
	return result;
}

} // namespace duckdb
