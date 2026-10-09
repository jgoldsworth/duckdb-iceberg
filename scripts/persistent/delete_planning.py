import os
import shutil

import pyspark
import pyspark.sql

DATA_GENERATION_DIR = 'data/persistent/delete_planning'
SCRIPT_DIR = os.path.dirname(__file__)
SPARK_RUNTIME_PATH = os.path.join(SCRIPT_DIR, '..', 'data_generators', 'iceberg-spark-runtime-3.5_2.12-1.9.0.jar')

shutil.rmtree(DATA_GENERATION_DIR, ignore_errors=True)

conf = pyspark.SparkConf()
conf.setMaster('local[1]')
conf.set('spark.sql.catalog.iceberg_catalog', 'org.apache.iceberg.spark.SparkCatalog')
conf.set('spark.sql.catalog.iceberg_catalog.type', 'hadoop')
conf.set('spark.sql.catalog.iceberg_catalog.warehouse', DATA_GENERATION_DIR)
conf.set('spark.sql.shuffle.partitions', '1')
conf.set('spark.jars', SPARK_RUNTIME_PATH)
conf.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("USE iceberg_catalog")

# Eight commits (eight data manifests) over four identity partitions of `part`.
# Commit c writes partition c % 4 with ts in [100c, 100c + 2], so per-file ts bounds are disjoint and ascending.
spark.sql(
    """
CREATE OR REPLACE TABLE pdel (id BIGINT, part INT, ts BIGINT, v STRING)
USING iceberg
PARTITIONED BY (part)
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
)
"""
)

for c in range(8):
    rows = ", ".join(f"({10 * c + i}, {c % 4}, {100 * c + i}, 'c{c}-{i}')" for i in range(3))
    spark.sql(f"INSERT INTO pdel VALUES {rows}")

# One delete file and one delete manifest per partition: delete the first row of commits 0..3 (ids 0, 10, 20, 30).
for k in range(4):
    spark.sql(f"DELETE FROM pdel WHERE part = {k} AND id = {10 * k}")

# A delete on a non-partition predicate that touches every partition but only the files of commits 4..7,
# so the first four delete manifests stay live next to it instead of being rewritten.
spark.sql("DELETE FROM pdel WHERE ts >= 400 AND v LIKE '%-2'")

spark.sql("SELECT * FROM pdel ORDER BY id").show(100)
