"""
Read CDC data from MSK, then apply to Redshift.
1. Read / Load Data from MSK cluster by MSK connection
2. batch transffer 
3. Sink Data to Redshift
4. Run the job like this:
aws glue start-job-run \
--job-name MongoCDCMSKPyJob \
--arguments='{"--S3_CONF_BUCKET": "configure bucket name","--S3_CONF_KEY": "path of glue-job.json"}'
"""
import sys

import redshift_connector

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, from_json, get_json_object, row_number, to_date, from_unixtime
from pyspark.sql.types import *

from redshift_connector.core import Connection

from concurrent.futures import ThreadPoolExecutor

import boto3
import json

#============config==============
args = getResolvedOptions(sys.argv, ['JOB_NAME', "S3_CONF_BUCKET", "S3_CONF_KEY"])
s3ConfBucket = args["S3_CONF_BUCKET"]
s3ConfKey = args["S3_CONF_KEY"]

s3 = boto3.client("s3")

confStr = None
confJson = None
try:
    confStr = s3.get_object(Bucket=s3ConfBucket, Key=s3ConfKey)['Body'].read()
    confJson = json.loads(confStr)
except:
    print(f"configuration '{s3ConfBucket}/{s3ConfKey}' not exist or get the s3 object failed")

RS_CONN_CONF = None
MSK_CONF = None
SPARK_BATCH_CONF = None

if not confJson == None:
    RS_CONN_CONF = confJson['redshift_conf']
    MSK_CONF = confJson['msk_conf']
    SPARK_BATCH_CONF = confJson['spark_batch_conf']
    SPARK_CONF = confJson['spark_conf']

    print(f"RS_CONN_CONF: {RS_CONN_CONF}")
    print(f"MSK_CONF: {MSK_CONF}")
    print(f"SPARK_BATCH_CONF: {SPARK_BATCH_CONF}")

if RS_CONN_CONF == None or MSK_CONF == None or SPARK_BATCH_CONF == None:
    print("===redshift or msk or spark batch conf is None, do nothing, exit===")
    sys.exit(0)

MONGO_CDC_SCHEMA = StructType([StructField("_id", StringType(), False), 
                               StructField("operationType", StringType(), True), 
                               StructField("fullDocument", StringType(), True),
                               StructField("source", StructType([
                                   StructField("ts_ms", LongType(), False),
                                   StructField("snapshot", StringType(), False)
                               ]), True),
                               StructField("ts_ms", LongType(), True),
                               StructField("ns", StructType([
                                   StructField("db", StringType(), False),
                                   StructField("coll", StringType(), False)
                               ]), True),
                               StructField("to", StructType([
                                   StructField("db", StringType(), False),
                                   StructField("coll", StringType(), False)
                               ]), True),
                               StructField("documentKey", StringType(), True),
                               StructField("updateDescription", StructType([
                                   StructField("updatedFields", StringType(), True),
                                   StructField("removedFields", ArrayType(StringType()), False)
                               ]), True),
                               StructField("clusterTime", StringType(), True),
                               StructField("txnNumber", LongType(), True),
                               StructField("lsid", StructType([
                                   StructField("id", StringType(), False),
                                   StructField("uid", StringType(), False)
                               ]), True)
                            ])

#==================================
def createSparkConf() -> SparkConf:
    conf_list = [
        #  ("spark.shuffle.service.enabled", "false"),
         ("spark.dynamicAllocation.enabled", SPARK_CONF['dynamicAllocationEnabled']),
         ("spark.executor.memory", SPARK_CONF['executorMemory']),
         ("spark.executor.cores", SPARK_CONF['executorCores']),
         ("spark.sql.shuffle.partitions", SPARK_CONF['shufflePartitions']),
         ("spark.default.parallelism", SPARK_CONF['defaultParallelism']),
         ("spark.speculation", SPARK_CONF['speculation'])
    ]
    spark_conf = SparkConf().setAll(conf_list)
    return  spark_conf

def checkRedshiftTableExists(schema: str, table: str, rfConn:  Connection) -> bool:
    sql = "select distinct tablename from pg_tables where tablename = '{tbName}' and schemaname='{schema}'".format(tbName=table, schema=schema)

    with rfConn.cursor() as cursor:
        cursor.execute(sql)
        res = cursor.fetchall()

        if res:
            return True
    
    return False

# df: json format, schema: 
# https://github.com/ververica/flink-cdc-connectors/blob/master/flink-connector-mongodb-cdc/src/main/java/com/ververica/cdc/connectors/mongodb/internal/MongoDBEnvelope.java

def mongoCDCBatchProcessor(df: DataFrame, batchId):

    print(f"processBatch: {batchId}")

    if df.rdd.count() <= 0:
        print(f"mongoCDCBatchProcessor: {batchId}, count size is 0")
        return
    
    logger.info("batch mongo cdc data syncing starts")
    
    df = df.withColumn("doc_id", get_json_object("documentKey", "$._id")) \
        .withColumn("db_name", col("ns").db) \
        .withColumn("tb_name", col("ns").coll)
    
    win = Window.partitionBy("db_name", "tb_name", "doc_id").orderBy(col("ts_ms").desc())
    df = df.withColumn("row_num", row_number().over(win)) \
        .where(col("row_num")==1) \
        .withColumn("ts_date",  to_date(from_unixtime(col("ts_ms") / 1000), 'yyyy-MM-dd HH:mm:ss')) \
        .select(col("doc_id"), \
                col("db_name").alias("db_name", metadata={'redshift_type': 'VARCHAR(120)'}), \
                col("tb_name").alias("tb_name", metadata={'redshift_type': 'VARCHAR(120)'}), \
                col("fullDocument").alias("doc", metadata={'redshift_type': 'SUPER'}), \
                col("ts_date"), \
                col("ts_ms"), \
                col("operationType").alias("op_type", metadata={'redshift_type': 'VARCHAR(64)'}))

    win2 = Window.partitionBy("db_name", "tb_name").orderBy(col("ts_ms"))
    dbAndTbls = df.withColumn("row_num", row_number().over(win2)) \
      .where(col("row_num")==1) \
      .select(col("db_name"), col("tb_name")).collect()
    
    def tableMapFunc(r: Row) -> tuple:

        dbName = r["db_name"]
        tbName = r["tb_name"]

        rfConn = redshift_connector.connect(
            host= RS_CONN_CONF['cluster'],
            database=RS_CONN_CONF['database'],
            port=RS_CONN_CONF['port'],
            user=RS_CONN_CONF['user'],
            password=RS_CONN_CONF['password']
        )

        logger.info("tableMapFunc initiated the redshift connection.")

        stageTable = f"{RS_CONN_CONF['schema']}.stage_{dbName}_{tbName}"
        targetTable = f"{RS_CONN_CONF['schema']}.{dbName}_{tbName}"
        targetTableWithoutSchema = f"{dbName}_{tbName}"

        toSaveDF = df.filter((col("db_name")==dbName) & (col("tb_name") == tbName))

        toSaveColumns = toSaveDF.columns
        toSaveColumns.remove("op_type")

        logger.info(f"targetTable: {targetTable}, stageTable:{stageTable}")

        createTableSql = "create table {target_table} sortkey (ts_date) as select {columns} from {stage_table} where 1=3;" \
            .format(target_table= targetTable, stage_table = stageTable, columns = ",".join(toSaveColumns))
        
        appendDataSql = "begin; delete from {target_table} using {stage_table} where {target_table}.{join_key} = {stage_table}.{join_key}; insert into {target_table} ({columns}) select {columns} from {stage_table} where op_type != '{op_delete_val}'; drop table {stage_table}; end;" \
            .format(target_table = targetTable, stage_table = stageTable, join_key = "doc_id", columns = ",".join(toSaveColumns), op_delete_val = "delete")
        
        postActionSql = appendDataSql

        if not checkRedshiftTableExists(RS_CONN_CONF['schema'], targetTableWithoutSchema, rfConn):
            postActionSql = appendDataSql.replace("begin;", "begin; {0}".format(createTableSql))
        
        toSaveDF.write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", RS_CONN_CONF["url"]) \
                .option("dbtable", stageTable) \
                .option("user", RS_CONN_CONF["user"]) \
                .option("password", RS_CONN_CONF["password"]) \
                .option("tempdir", RS_CONN_CONF["tmpdir"]) \
                .option("postactions", postActionSql) \
                .option("tempformat", "CSV") \
                .option("aws_iam_role", RS_CONN_CONF["aws_iam_role"]) \
                .mode("append") \
                .save()
        
        return (dbName, tbName, True)
    
    with ThreadPoolExecutor(max_workers=10, thread_name_prefix="spark_batch") as executor:
        ret = executor.map(tableMapFunc, dbAndTbls)
        for r in ret:
            logger.info(f"======>db: {r[0]} , table: {r[1]} finished batch processing <======")

#==================================
sparkConf = createSparkConf()
sc = SparkContext(conf=sparkConf)
glueCtx = GlueContext(sc)
logger = glueCtx.get_logger()
spark = glueCtx.spark_session
job = Job(glueCtx)
job.init(args['JOB_NAME'], args)

logger.info("MongoCDCMSKJob init the job...")

mskDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", MSK_CONF["brokers"]) \
    .option("subscribe",MSK_CONF["topic"]) \
    .option("startingOffsets",MSK_CONF["startingOffsets"]) \
    .option("kafka.consumer.commit.groupid", MSK_CONF["groupId"]) \
    .option("kafkaConsumer.pollTimeoutMs", MSK_CONF["pollTimeoutMs"]) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), MONGO_CDC_SCHEMA).alias("json")) \
    .select(col("json.*"))

logger.info(f"mskDF schema: {mskDF.schema}")
logger.info(f"mskDF columns: {mskDF.columns}")


logger.info("MongoCDCMSKJob make the msk dataframe...")

glueCtx.forEachBatch(frame=mskDF, \
                     batch_function=mongoCDCBatchProcessor, \
                     options=SPARK_BATCH_CONF \
                    )
logger.info("MongoCDCMSKJob init the mirco batch processing...")

job.commit()