import os
import shutil

import pyspark
import pyspark.sql

DATA_GENERATION_DIR = 'data/persistent/delete_scan_order'
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

# overlap: format v3 (deletion vectors), three data files of 5 rows whose ts ranges interleave
# (0..12 step 3, 1..13 step 3, 2..14 step 3). The rows with ts 0, 1 and 2 are deleted, 12 live rows.
spark.sql(
    """
CREATE OR REPLACE TABLE overlap (id BIGINT, ts BIGINT)
USING iceberg
TBLPROPERTIES (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.delete.granularity' = 'file'
)
"""
)
for c in range(3):
    rows = ", ".join(f"({10 * c + i}, {3 * i + c})" for i in range(5))
    spark.sql(f"INSERT INTO overlap VALUES {rows}")
spark.sql("DELETE FROM overlap WHERE ts < 3")


# parted: format v3, identity(part), four data files of 5 rows (part = commit, ts = 100 * commit + [0, 4], ascending
# and disjoint), one deletion vector and one delete manifest per partition: the row with ts = 100 * part is deleted.
spark.sql(
    """
CREATE OR REPLACE TABLE parted (id BIGINT, part INT, ts BIGINT)
USING iceberg
PARTITIONED BY (part)
TBLPROPERTIES (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.delete.granularity' = 'file'
)
"""
)
for c in range(4):
    rows = ", ".join(f"({10 * c + i}, {c}, {100 * c + i})" for i in range(5))
    spark.sql(f"INSERT INTO parted VALUES {rows}")
for c in range(4):
    spark.sql(f"DELETE FROM parted WHERE part = {c} AND id = {10 * c}")

spark.sql("SELECT * FROM overlap ORDER BY ts").show(100)
spark.sql("SELECT * FROM parted ORDER BY ts").show(100)
