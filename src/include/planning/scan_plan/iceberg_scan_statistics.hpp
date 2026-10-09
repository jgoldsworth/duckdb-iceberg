#pragma once

#include "core/deletes/iceberg_delete_file.hpp"
#include "planning/metadata_io/manifest_list/bound_iceberg_manifest_list_entry.hpp"

namespace duckdb {

//! Count policy for a pruned manifest view. Does not load manifests or advance scan cursors.
//! Estimates may account for delete counts; exact counts require no matching delete manifests, or proof per data file
//! that the applicable deletes remove a known number of rows.
class IcebergScanStatistics {
public:
	IcebergScanStatistics(const vector<BoundIcebergManifestListEntry> &data_manifests,
	                      const vector<bool> &data_manifest_matches,
	                      const vector<BoundIcebergManifestListEntry> &delete_manifests,
	                      const vector<bool> &delete_manifest_matches);

	optional<idx_t> EstimateCardinality() const;
	optional<idx_t> ExactRowCount() const;
	//! Rows removed from a data file by its applicable delete files, or nullopt when that number is not provable from
	//! manifest metadata alone: equality deletes, several position delete files or deletion vectors, and position
	//! delete files that are not scoped to this data file all decline.
	static optional<idx_t> ExactDeletedRows(const string &data_file_path, const vector<IcebergDeleteFile> &deletes);

private:
	optional<idx_t> CountDataRows() const;

	const vector<BoundIcebergManifestListEntry> &data_manifests;
	const vector<bool> &data_manifest_matches;
	const vector<BoundIcebergManifestListEntry> &delete_manifests;
	const vector<bool> &delete_manifest_matches;
};

} // namespace duckdb
