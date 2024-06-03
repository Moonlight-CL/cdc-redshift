"""
A pyspark job script: read dms cdc data, and write the data to Amazon Redshift
aws glue start-job-run \
--job-name ShardingCDCToRedshift \
--region us-west-2 \
--arguments='{"--S3_CONF_BUCKET": "bkt","--S3_CONF_KEY": "key/of/conf.json"}'
"""

import sys
from concurrent.futures import ThreadPoolExecutor
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
import boto3.s3
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from awsglue.job import Job
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructField, StructType, LongType, StringType, Row
from pyspark.sql.functions import row_number

import boto3
import json

args = getResolvedOptions(sys.argv, ['JOB_NAME',"S3_CONF_BUCKET", "S3_CONF_KEY"])
conf_bucket = args["S3_CONF_BUCKET"]
conf_key = args["S3_CONF_KEY"]

sc = SparkContext(appName="dmscdc-msk-redshift")
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glue_context=glueContext)
job.init(job_name=args['JOB_NAME'], args=args)

# global configs 
record_id_key = "id"
cdc_timestamp_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
apply_delete = True
msk_conf = {}
spark_batch_conf = {}
redshift_conf = {}

# dms cdc data format
dms_cdc_schema = StructType([
    StructField("data", StringType(), True),
    StructField("control", StringType(), True),
    StructField("metadata", StructType([
        StructField("timestamp", StringType(), True),
        StructField("record-type", StringType(), True),
        StructField("operation", StringType(), False),
        StructField("partition-key-type", StringType(), True),
        StructField("schema-name", StringType(), True),
        StructField("table-name", StringType(), False)
    ]), False)
])

# read config from s3, and init the global config
def init_config(bucket=conf_bucket, key = conf_key):
    s3 = boto3.client("s3")
    config = s3.get_object(Bucket=bucket, Key=key).get("Body").read()
    config_json = json.loads(config)

    global msk_conf, redshift_conf, spark_batch_conf, record_id_key, apply_delete, cdc_timestamp_format
    msk_conf = config_json["msk_conf"]
    redshift_conf = config_json["redshift_conf"]
    spark_batch_conf = config_json["spark_batch_conf"]
    record_id_key = config_json["record_id_key"]
    apply_delete = config_json["apply_delete"]
    cdc_timestamp_format = config_json["cdc_timestamp_format"]

    logger.info(f"{msk_conf=},\n {redshift_conf=},\n {spark_batch_conf=}, \n {record_id_key=}, {apply_delete=}, {cdc_timestamp_format=}")

def run():
    init_config()
    # read cdc data from msk, filter the emplty text line,
    # covert to json schema and select the data field
    logger.info("read cdc data from msk.....")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", msk_conf["brokers"]) \
        .option("subscribe", msk_conf["topic"]) \
        .option("startingOffsets", msk_conf["startingOffsets"]) \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .filter(col("value").isNotNull()) \
        .select(from_json(col("value"), dms_cdc_schema).alias("json")) \
        .select(col("json.*")) \
        .filter(col("metadata.record-type") == "data") \

    logger.info(f"msk df schema: {df.schema}")

    glueContext.forEachBatch(frame=df,
                             batch_function=processBatch,
                             options=spark_batch_conf
                             )
    job.commit()

def get_save_redshift_options(stage_table: str, table_alias: str,
                               target_table: str, op: str) -> dict:

    post_action_sql = ""

    if op != 'delete':
        post_action_sql = f"""
            BEGIN;
            CREATE TABLE IF NOT EXISTS {target_table} (PRIMARY KEY(id), LIKE {stage_table});
            MERGE INTO {target_table} USING {stage_table} s ON ({table_alias}.id = s.id) REMOVE DUPLICATES;
            TRUNCATE TABLE {stage_table};
            COMMIT;
        """
    else:
        post_action_sql = f"""
            BEGIN;
            DELETE FROM {target_table} USING {stage_table} WHERE {target_table}.id = {stage_table}.id;
            TRUNCATE TABLE {stage_table};
            COMMIT;
        """

    return {
        "url": redshift_conf["url"],
        "user": redshift_conf["user"],
        "password": redshift_conf["password"],
        "dbtable": stage_table,
        "tempdir": redshift_conf["tmpDir"],
        "aws_iam_role": redshift_conf["aws_iam_role"],
        "tempformat": "CSV",
        "postactions": post_action_sql
    }

def processBatch(df: DataFrame, batchId):
    if df.count() == 0:
        logger.info(f"batch data is empty, skip this batch: {batchId}")
        return

    logger.info(f"start processing batch: {batchId}")
    # filter the data which is not delete record type and spread out the data json
    # df = df.filter(col("metadata.operation") != 'delete') \
    df = df.select(to_timestamp(col("metadata.timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias('ts'), \
                col("metadata.schema-name").alias("db"), 
                col("metadata.table-name").alias("tb"), 
                col("metadata.operation").alias("op"), 
                col("data").alias("data")) 

    # get db and tb name from the data
    win = Window.partitionBy("db", "tb").orderBy(col("ts"))
    db_tabs = df.withColumn("row_num", row_number().over(win)) \
        .where(col("row_num") == 1) \
        .select(col("db"), col("tb")).collect()

    # per tabel save function
    def tableSaveFunc(db_tab: Row) -> tuple:
        db = db_tab["db"]
        tab = db_tab["tb"]
        logger.info(f"start to process the table: {db}.{tab}")

        rf_stage_table = f"{redshift_conf['schema']}.stage_{tab}"
        rf_target_table = f"{redshift_conf['schema']}.{tab}"

        to_save_df = df.filter((col("db") == db) & (col("tb") == tab)) \
               .select(col("data"), col("ts"), col("op"))
        data_schema = spark.read.json(to_save_df.rdd.map(lambda r: r.data)).schema

        to_save_df = to_save_df.select(from_json(col("data"), data_schema).alias("d"), col("ts"), col("op")) \
            .select("d.*", "ts", "op") \
            .withColumn("gmt_created", to_timestamp("gmt_created", cdc_timestamp_format)) \
            .withColumn("gmt_modified", to_timestamp("gmt_modified", cdc_timestamp_format))
        
        logger.info(f"to_save_df: {to_save_df.columns}")

        dedup_win = Window.partitionBy("id").orderBy(col("ts").desc())
        to_save_df = to_save_df.withColumn("row_num", row_number().over(dedup_win)) \
            .where(col("row_num") == 1)
        
        # apply load / insert / update changes
        to_save_df.filter(col("op") != "delete") \
            .drop("row_num", "ts", "op") \
            .write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .options(**get_save_redshift_options(rf_stage_table, tab, rf_target_table, "load|insert|update")) \
            .mode("append") \
            .save()
        
        # apply delete changes if need
        if apply_delete:
            del_df = to_save_df.filter(col("op") == "delete")
            if del_df.count() > 0:
                logger.info(f"apply delete changes for table: {db}.{tab}")
                del_df.drop("row_num", "ts", "op") \
                    .write \
                    .format("io.github.spark_redshift_community.spark.redshift") \
                    .options(**get_save_redshift_options(rf_stage_table, tab, rf_target_table, "delete")) \
                    .mode("append") \
                    .save()

        return (db, tab, True)

    with ThreadPoolExecutor(max_workers=3, thread_name_prefix="spark_batch") as executor:
        ret = executor.map(tableSaveFunc, db_tabs)
        for r in ret:
            logger.info(
                f"======>db: {r[0]} , table: {r[1]} finished batch:{batchId} processing <======")


if __name__ == "__main__":
    run()
    
