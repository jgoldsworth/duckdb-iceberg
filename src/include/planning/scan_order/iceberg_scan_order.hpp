#pragma once

#include "planning/metadata_io/manifest/bound_iceberg_manifest_entry.hpp"

#include <functional>

namespace duckdb {

class IcebergTableSchema;
struct RowGroupOrderOptions;

class IcebergScanOrder {
public:
	IcebergScanOrder();
	~IcebergScanOrder();

	void Set(unique_ptr<RowGroupOrderOptions> options);
	unique_ptr<RowGroupOrderOptions> CopyOptions() const;
	optional_ptr<const RowGroupOrderOptions> GetOptions() const;
	bool IsPending() const;

	void MarkApplied();

	static void Apply(ClientContext &context, const IcebergTableSchema &schema, const RowGroupOrderOptions &options,
	                  vector<BoundIcebergManifestEntry> &manifest_entries,
	                  const std::function<idx_t(const BoundIcebergManifestEntry &)> &live_row_count);

private:
	unique_ptr<RowGroupOrderOptions> options;
	bool applied = false;
};

} // namespace duckdb
