#!/usr/bin/env python3
"""
Minimal mock REST catalog that answers server-side scan planning with canned plans.

Namespace `default` holds two tables that require server-side planning and whose files do not exist:
- `typed`: a few files with null, binary and typed bounds and partition values.
- `wide`: FILES data files with COLUMNS long columns, every column with hex bounds,
  partitioned by identity on the first column.

Usage:
    python3 scan_planning_mock_server.py [port] [files] [columns]
    Defaults: 19131 10000 50
"""

import json
import struct
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

TABLE_PREFIX = "/v1/namespaces/default/tables/"
TABLE_UUIDS = {"typed": "00000000-0000-0000-0000-000000000001", "wide": "00000000-0000-0000-0000-000000000002"}


def hex_of(fmt, value):
    return struct.pack(fmt, value).hex().upper()


def metadata(name, fields, partition_fields):
    return {
        "format-version": 2,
        "table-uuid": TABLE_UUIDS[name],
        "location": f"/scan-planning-mock/{name}",
        "last-updated-ms": 0,
        "last-column-id": len(fields),
        "last-sequence-number": 1,
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": fields}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": partition_fields}],
        "last-partition-id": 999 + len(partition_fields),
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "current-snapshot-id": 1,
        "snapshots": [
            {
                "snapshot-id": 1,
                "sequence-number": 1,
                "timestamp-ms": 0,
                "manifest-list": f"/scan-planning-mock/{name}/metadata/snap-1.avro",
                "summary": {"operation": "append"},
                "schema-id": 0,
            }
        ],
        "snapshot-log": [],
        "metadata-log": [],
    }


def data_file(path, partition, lower_bounds, upper_bounds):
    keys = list(lower_bounds)
    return {
        "data-file": {
            "content": "data",
            "file-path": f"/scan-planning-mock/{path}.parquet",
            "file-format": "parquet",
            "spec-id": 0,
            "partition": partition,
            "file-size-in-bytes": 1024,
            "record-count": 10,
            "lower-bounds": {"keys": keys, "values": [lower_bounds[k] for k in keys]},
            "upper-bounds": {"keys": keys, "values": [upper_bounds[k] for k in keys]},
        }
    }


def typed_table():
    fields = [
        {"id": 1, "name": "i", "required": False, "type": "int"},
        {"id": 2, "name": "l", "required": False, "type": "long"},
        {"id": 3, "name": "d", "required": False, "type": "date"},
        {"id": 4, "name": "s", "required": False, "type": "string"},
        {"id": 5, "name": "v", "required": False, "type": "long"},
        {"id": 6, "name": "b", "required": False, "type": "binary"},
        {"id": 7, "name": "n", "required": False, "type": "long"},
    ]
    partition_fields = [
        {"name": "i", "transform": "identity", "source-id": 1, "field-id": 1000},
        {"name": "l", "transform": "identity", "source-id": 2, "field-id": 1001},
        {"name": "d", "transform": "identity", "source-id": 3, "field-id": 1002},
        {"name": "s", "transform": "identity", "source-id": 4, "field-id": 1003},
    ]
    files = [
        data_file(
            "typed_a",
            [1234, 10000000000, "2024-03-01", "1234"],
            {5: hex_of("<q", 10), 6: "CAFE", 7: None},
            {5: hex_of("<q", 20), 6: "CAFF", 7: None},
        ),
        data_file(
            "typed_b",
            [None, None, None, None],
            {5: hex_of("<q", 30), 6: "00", 7: hex_of("<q", -5)},
            {5: hex_of("<q", 40), 6: "01", 7: hex_of("<q", 5)},
        ),
    ]
    return metadata("typed", fields, partition_fields), files


def wide_table(file_count, column_count):
    fields = [{"id": c, "name": f"c{c}", "required": False, "type": "long"} for c in range(1, column_count + 1)]
    partition_fields = [{"name": "c1", "transform": "identity", "source-id": 1, "field-id": 1000}]
    files = []
    for f in range(file_count):
        lower = {field["id"]: hex_of("<q", f * 1000 + field["id"]) for field in fields}
        upper = {field["id"]: hex_of("<q", f * 1000 + field["id"] + 500) for field in fields}
        files.append(data_file(f"wide_{f}", [f], lower, upper))
    return metadata("wide", fields, partition_fields), files


def build_tables(file_count, column_count):
    tables = {}
    for name, (table_metadata, files) in (
        ("typed", typed_table()),
        ("wide", wide_table(file_count, column_count)),
    ):
        load = {
            "metadata-location": f"/scan-planning-mock/{name}/metadata/v1.metadata.json",
            "metadata": table_metadata,
            "config": {"scan-planning-mode": "server"},
        }
        plan = {"status": "completed", "plan-id": f"plan-{name}", "delete-files": [], "file-scan-tasks": files}
        tables[name] = (json.dumps(load).encode(), json.dumps(plan).encode())
    return tables


CONFIG = {
    "defaults": {},
    "overrides": {},
    "endpoints": [
        "GET /v1/{prefix}/namespaces",
        "GET /v1/{prefix}/namespaces/{namespace}",
        "GET /v1/{prefix}/namespaces/{namespace}/tables",
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
    ],
}


class MockCatalogHandler(BaseHTTPRequestHandler):
    tables = {}

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/v1/config":
            self._respond(200, json.dumps(CONFIG).encode())
        elif path == "/v1/namespaces":
            self._respond(200, json.dumps({"namespaces": [["default"]]}).encode())
        elif path == "/v1/namespaces/default":
            self._respond(200, json.dumps({"namespace": ["default"], "properties": {}}).encode())
        elif path == "/v1/namespaces/default/tables":
            identifiers = [{"namespace": ["default"], "name": name} for name in self.tables]
            self._respond(200, json.dumps({"identifiers": identifiers}).encode())
        elif path.startswith(TABLE_PREFIX) and path[len(TABLE_PREFIX) :] in self.tables:
            self._respond(200, self.tables[path[len(TABLE_PREFIX) :]][0])
        else:
            self._not_found()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        path = self.path.split("?", 1)[0]
        name = path[len(TABLE_PREFIX) : -len("/plan")] if path.endswith("/plan") else None
        if path.startswith(TABLE_PREFIX) and name in self.tables:
            self._respond(200, self.tables[name][1])
        else:
            self._not_found()

    def _not_found(self):
        error = {"error": {"message": f"Not found: {self.path}", "type": "NoSuchTableException", "code": 404}}
        self._respond(404, json.dumps(error).encode())

    def _respond(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        pass


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19131
    file_count = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    column_count = int(sys.argv[3]) if len(sys.argv) > 3 else 50
    MockCatalogHandler.tables = build_tables(file_count, column_count)
    server = ThreadingHTTPServer(("127.0.0.1", port), MockCatalogHandler)
    print(f"Mock scan-planning catalog server listening on http://127.0.0.1:{port}")
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
