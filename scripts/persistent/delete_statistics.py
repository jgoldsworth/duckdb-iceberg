import glob
import os
import shutil

import fastavro

import pyspark
import pyspark.sql

DATA_GENERATION_DIR = 'data/persistent/delete_statistics'
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


def create_table(name, version, granularity):
    spark.sql(
        f"""
CREATE OR REPLACE TABLE {name} (id BIGINT, v STRING)
USING iceberg
TBLPROPERTIES (
    'format-version' = '{version}',
    'write.delete.mode' = 'merge-on-read',
    'write.delete.granularity' = '{granularity}'
)
"""
    )
    for c in range(3):
        rows = ", ".join(f"({10 * c + i}, 'c{c}-{i}')" for i in range(5))
        spark.sql(f"INSERT INTO {name} VALUES {rows}")


# Three data files of 5 rows each (ids 0-4, 10-14, 20-24). dv: format v3, so every delete is a deletion vector; the
# third delete merges into the first file's vector. 12 live rows.
create_table('dv', 3, 'file')
spark.sql("DELETE FROM dv WHERE id = 1")
spark.sql("DELETE FROM dv WHERE id = 12")
spark.sql("DELETE FROM dv WHERE id = 2")

# refpos: v2 position deletes that each hold the rows of one data file. 12 live rows. The third data file is untouched.
# Spark leaves referenced_data_file unset on v2 deletes, so the manifests are patched below to name the data file.
create_table('refpos', 2, 'file')
spark.sql("DELETE FROM refpos WHERE id IN (1, 2)")
spark.sql("DELETE FROM refpos WHERE id = 12")

# refpos_overlap: as refpos, but a later delete on the first data file adds a second position delete file that names it.
create_table('refpos_overlap', 2, 'partition')
spark.sql("DELETE FROM refpos_overlap WHERE id = 1")
spark.sql("DELETE FROM refpos_overlap WHERE id = 12")
spark.sql("DELETE FROM refpos_overlap WHERE id = 2")



def set_referenced_data_file(table):
    for manifest in glob.glob(f'{DATA_GENERATION_DIR}/{table}/metadata/*-m*.avro'):
        with open(manifest, 'rb') as f:
            reader = fastavro.reader(f)
            metadata = reader.metadata
            schema = reader.writer_schema
            records = list(reader)
        if metadata.get('content') != 'deletes':
            continue
        for record in records:
            data_file = record['data_file']
            referenced = {
                row.file_path
                for row in spark.read.parquet(data_file['file_path']).select('file_path').distinct().collect()
            }
            assert len(referenced) == 1, referenced
            data_file['referenced_data_file'] = referenced.pop()
        with open(manifest, 'wb') as f:
            fastavro.writer(f, schema, records, metadata=metadata, codec='deflate')


for t in ('refpos', 'refpos_overlap'):
    set_referenced_data_file(t)
    for checksum in glob.glob(f'{DATA_GENERATION_DIR}/{t}/metadata/.*.crc'):
        os.remove(checksum)

for t in ('dv', 'refpos', 'refpos_overlap'):
    spark.sql(f"SELECT * FROM {t} ORDER BY id").show(100)
