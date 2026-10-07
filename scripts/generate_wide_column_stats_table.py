#!/usr/bin/env python3
"""
Generate a metadata-only Iceberg table for benchmark/manifest_planning: 8192 data files spread over 8
manifests, each with column sizes, value/null/NaN counts and bounds for 128 columns, partitioned by
identity(part). The data files do not exist, so only queries that prune every file can run against it.
"""

import os
import shutil
import struct
import uuid

from pyiceberg.io import load_file_io
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestListWriterV2,
    ManifestWriterV2,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadataV2
from pyiceberg.table.refs import SnapshotRef
from pyiceberg.table.snapshots import Snapshot, SnapshotLogEntry
from pyiceberg.table.sorting import SortOrder
from pyiceberg.transforms import IdentityTransform
from pyiceberg.typedef import Record
from pyiceberg.types import DoubleType, LongType, NestedField

OUT_DIR = os.path.join("data", "generated", "wide_column_stats")
META_DIR = os.path.join(OUT_DIR, "metadata")

SNAPSHOT_ID = 1000000000000000001
MANIFESTS = 8
FILES_PER_MANIFEST = 1024
COLUMNS = 128
ROWS_PER_FILE = 1000

if os.path.exists(OUT_DIR):
    shutil.rmtree(OUT_DIR)
os.makedirs(META_DIR)

# Field 1 is the partition source; fields 2..COLUMNS alternate BIGINT and DOUBLE so NaN counts are populated.
fields = [NestedField(1, "part", LongType(), required=False)]
for field_id in range(2, COLUMNS + 1):
    field_type = DoubleType() if field_id % 2 else LongType()
    fields.append(NestedField(field_id, f"c{field_id:03d}", field_type, required=False))
schema = Schema(*fields, schema_id=0)
spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="part"), spec_id=0)
io = load_file_io({"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"})


def encode(field_id, value):
    return struct.pack("<d", float(value)) if field_id % 2 and field_id != 1 else struct.pack("<q", value)


manifest_files = []
for m in range(MANIFESTS):
    manifest_path = os.path.join(META_DIR, f"manifest-m{m}.avro")
    with ManifestWriterV2(spec, schema, io.new_output(manifest_path), SNAPSHOT_ID, "null") as writer:
        for f in range(FILES_PER_MANIFEST):
            file_index = m * FILES_PER_MANIFEST + f
            lower_value = file_index * ROWS_PER_FILE
            upper_value = lower_value + ROWS_PER_FILE - 1
            field_ids = range(1, COLUMNS + 1)
            data_file = DataFile.from_args(
                _table_format_version=2,
                content=DataFileContent.DATA,
                file_path=f"s3://fake-bucket/wide_column_stats/part={file_index}/{file_index:010d}.parquet",
                file_format=FileFormat.PARQUET,
                partition=Record(file_index),
                record_count=ROWS_PER_FILE,
                file_size_in_bytes=ROWS_PER_FILE * COLUMNS * 8,
                column_sizes={i: ROWS_PER_FILE * 8 for i in field_ids},
                value_counts={i: ROWS_PER_FILE for i in field_ids},
                null_value_counts={i: 0 for i in field_ids},
                nan_value_counts={i: 0 for i in field_ids if i % 2 and i != 1},
                lower_bounds={i: encode(i, lower_value) for i in field_ids},
                upper_bounds={i: encode(i, upper_value) for i in field_ids},
                split_offsets=[4],
                sort_order_id=0,
            )
            writer.add_entry(ManifestEntry(ManifestEntryStatus.ADDED, SNAPSHOT_ID, None, None, data_file))
    manifest_files.append(writer.to_manifest_file())

manifest_list_path = os.path.join(META_DIR, f"snap-{SNAPSHOT_ID}.avro")
with ManifestListWriterV2(io.new_output(manifest_list_path), SNAPSHOT_ID, None, 1, "null") as writer:
    writer.add_manifests(manifest_files)

metadata = TableMetadataV2(
    location=OUT_DIR,
    table_uuid=uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-000000000128"),
    last_updated_ms=1700000000000,
    last_column_id=COLUMNS,
    schemas=[schema],
    partition_specs=[spec],
    last_partition_id=1000,
    properties={},
    current_snapshot_id=SNAPSHOT_ID,
    snapshots=[
        Snapshot(
            snapshot_id=SNAPSHOT_ID,
            sequence_number=1,
            timestamp_ms=1700000000000,
            manifest_list=manifest_list_path,
            summary={"operation": "append"},
            schema_id=0,
        )
    ],
    snapshot_log=[SnapshotLogEntry(snapshot_id=SNAPSHOT_ID, timestamp_ms=1700000000000)],
    metadata_log=[],
    sort_orders=[SortOrder(order_id=0)],
    refs={"main": SnapshotRef(snapshot_id=SNAPSHOT_ID, snapshot_ref_type="branch")},
    statistics=[],
    partition_statistics=[],
    last_sequence_number=1,
)

with open(os.path.join(META_DIR, "v1.metadata.json"), "w") as f:
    f.write(metadata.model_dump_json(indent=2))
with open(os.path.join(META_DIR, "version-hint.text"), "w") as f:
    f.write("1")

print(f"Generated {MANIFESTS * FILES_PER_MANIFEST} data files in {MANIFESTS} manifests, {COLUMNS} columns: {OUT_DIR}")
