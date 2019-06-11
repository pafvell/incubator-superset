#encoding=utf-8
import sys, json
import argparse
import os
import logging
from datetime import datetime, timedelta

from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, when
from pyspark.storagelevel import StorageLevel

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='format data')
    parser.add_argument('input', help="")
    parser.add_argument('output_json', help="")
    parser.add_argument('output_parquet', help="")
    parser.add_argument('--is_druid', action='store_true', help='is process for druid import')
    args = parser.parse_args()

    sc = SparkContext(appName="format and merge data")
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc).builder.getOrCreate()
    data = spark.read.json(args.input).coalesce(1).cache()
    if args.is_druid:
        data.write.mode('overwrite').json(args.output_json)
    else:
        data.write.mode('overwrite').json(args.output_json, compression = 'com.hadoop.compression.lzo.LzopCodec')
        data.write.mode('overwrite').parquet(args.output_parquet)

    sc.stop()