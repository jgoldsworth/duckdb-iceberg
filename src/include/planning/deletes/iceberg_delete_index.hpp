#pragma once

#include "core/metadata/manifest/iceberg_manifest.hpp"
#include "planning/scan_plan/iceberg_scan_plan_context.hpp"

namespace duckdb {

struct IcebergTableMetadata;

//! Candidate generator for delete matching. It only narrows which delete entries are worth the exact
//! IcebergFilePruner::DeleteFileMatchesDataFile check, so a hash collision adds candidates but never drops one.
//! Not thread safe: guarded by the lock of the planner that owns it.
class IcebergDeleteIndex {
public:
	explicit IcebergDeleteIndex(const IcebergTableMetadata &metadata);

	//! Memo of the delete manifests that IcebergDeletePlanner::GetDeleteManifestsForDataFile selects for data files
	//! with the same manifest, sequence number and partition tuple.
	optional_ptr<const vector<idx_t>> FindManifestCandidates(idx_t data_manifest_idx, int64_t data_sequence_number,
	                                                         const vector<IcebergPartitionInfo> &data_partition) const;
	void StoreManifestCandidates(idx_t data_manifest_idx, int64_t data_sequence_number,
	                             const vector<IcebergPartitionInfo> &data_partition, vector<idx_t> candidates);

	vector<idx_t> UnindexedManifests(const vector<idx_t> &manifest_indexes) const;
	void AddEntry(const IcebergDeleteFileReference &reference, int32_t delete_spec_id,
	              const IcebergDataFile &delete_file);
	void MarkIndexed(const vector<idx_t> &manifest_indexes);

	//! Entries of the given (sorted, indexed) manifests that can apply to the data file, ordered by manifest and entry.
	vector<IcebergDeleteFileReference> FindEntryCandidates(const vector<idx_t> &manifest_indexes,
	                                                       const string &data_file_path, int32_t data_spec_id,
	                                                       const vector<IcebergPartitionInfo> &data_partition) const;

private:
	struct ManifestMemoEntry {
		idx_t data_manifest_idx;
		int64_t data_sequence_number;
		vector<IcebergPartitionInfo> data_partition;
		vector<idx_t> candidates;
	};

	optional<hash_t> PartitionKey(int32_t spec_id, const vector<IcebergPartitionInfo> &partition) const;

private:
	const IcebergTableMetadata &metadata;
	unordered_map<hash_t, vector<ManifestMemoEntry>> manifest_memo;
	unordered_set<idx_t> indexed_manifests;
	unordered_map<string, vector<IcebergDeleteFileReference>> by_referenced_data_file;
	unordered_map<hash_t, vector<IcebergDeleteFileReference>> by_partition;
	vector<IcebergDeleteFileReference> unpartitioned;
};

} // namespace duckdb
