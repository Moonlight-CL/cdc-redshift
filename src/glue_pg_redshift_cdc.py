"""
Read CDC data from MSK, then apply to Redshift.
1. Read / Load Data from MSK cluster by MSK connection
2. batch transffer 
3. Sink Data to Redshift
4. run the job by: 

aws glue start-job-run \
--job-name PgCDCMSKPyJob \
--arguments='{"--S3_CONF_BUCKET": "bkt-useast2-0604","--S3_CONF_KEY": "conf/glue-job-pg.json"}'
"""
import sys

import redshift_connector

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, from_json, get_json_object, row_number, to_date, from_unixtime, when
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
SPARK_CONF = None

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

# debezium 去除 schema 部分 的 数据格式,  为了处理方便, Flink cdc job 将before 和 after 字段以 String 类型的方式返回
CDC_SCHEMA = StructType([
    StructField("before", StringType(), True),
    StructField("after", StringType(), True),
    StructField("source", StructType([
        StructField("version", StringType(), False),
        StructField("connector", StringType(), False),
        StructField("name", StringType(), False),
        StructField("ts_ms", LongType(), False),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), False),
        StructField("sequence", StringType(), True),
        StructField("schema", StringType(), False),
        StructField("table", StringType(), False),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", LongType(), True),
    ]), False),
    StructField("op", StringType(), False),
    StructField("ts_ms", LongType(), True),
    StructField("transaction", StringType(), True),
])

#===============functions==================
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


def cdcBatchProcessor(df: DataFrame, batchId):

    print(f"processBatch: {batchId}")

    if df.rdd.count() <= 0:
        print(f"cdcBatchProcessor: {batchId}, count size is 0")
        return
    
    logger.info("batch cdc data syncing starts...")
    df.show(2)

    df = df.withColumn("pk_id", when(df['after'].isNotNull(), get_json_object("after", "$.id"))
                                .otherwise(get_json_object("before", "$.id"))) \
           .withColumn("db_name", col("source.db")) \
           .withColumn("tb_name", col("source.table")) \
           .withColumn("cts_ms", col("source.ts_ms"))
    
    win = Window.partitionBy("db_name", "tb_name", "pk_id").orderBy(col("cts_ms").desc())
    df = df.withColumn("row_num", row_number().over(win)) \
        .where(col("row_num")==1) \
        .withColumn("ts_date",  to_date(from_unixtime(col("cts_ms") / 1000), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn("data", when(col("after").isNotNull(), col("after")).otherwise(col("before"))) \
        .select(col("pk_id").alias("pk_id", metadata={'redshift_type': 'INT4'}), \
                col("db_name").alias("db_name", metadata={'redshift_type': 'VARCHAR(120)'}), \
                col("tb_name").alias("tb_name", metadata={'redshift_type': 'VARCHAR(120)'}), \
                col("data").alias("data", metadata={'redshift_type': 'SUPER'}), \
                col("ts_date"), \
                col("cts_ms"), \
                col("op").alias("op_type", metadata={'redshift_type': 'CHAR(2)'}))

    win2 = Window.partitionBy("db_name", "tb_name").orderBy(col("cts_ms"))
    dbAndTbls = df.withColumn("row_num", row_number().over(win2)) \
      .where(col("row_num")==1) \
      .select(col("db_name"), col("tb_name")).collect()
    
    def tableMapFunc(r: Row) -> tuple:

        dbName = r["db_name"]
        tbName = r["tb_name"]

        rfConn = redshift_connector.connect(
            host=RS_CONN_CONF['cluster'],
            database=RS_CONN_CONF['database'],
            port=RS_CONN_CONF['port'],
            user=RS_CONN_CONF['user'],
            password=RS_CONN_CONF['password']
        )

        logger.info(f"redshift write task: dbName:{dbName}, tbName: {tbName}, and tableMapFunc initiated")

        stageTable = f"{RS_CONN_CONF['schema']}.stage_{dbName}_{tbName}"
        targetTable = f"{RS_CONN_CONF['schema']}.{dbName}_{tbName}"
        targetTableWithoutSchema = f"{dbName}_{tbName}"
        tempDir = f"{RS_CONN_CONF['tmpdir']}{targetTableWithoutSchema}"

        toSaveDF = df.filter((col("db_name")==dbName) & (col("tb_name") == tbName))

        toSaveColumns = toSaveDF.columns
        toSaveColumns.remove("op_type")

        logger.info(f"targetTable: {targetTable}, stageTable:{stageTable}, tempDir:{tempDir}")

        createTableSql = "create table {target_table} sortkey (ts_date) as select {columns} from {stage_table} where 0=1;" \
            .format(target_table= targetTable, stage_table = stageTable, columns = ",".join(toSaveColumns))
        
        appendDataSql = "begin; delete from {target_table} using {stage_table} where {target_table}.{join_key} = {stage_table}.{join_key}; insert into {target_table} ({columns}) select {columns} from {stage_table} where op_type != '{op_delete_val}'; drop table {stage_table}; end;" \
            .format(target_table = targetTable, stage_table = stageTable, join_key = "pk_id", columns = ",".join(toSaveColumns), op_delete_val = "d")
        
        postActionSql = appendDataSql

        if not checkRedshiftTableExists(RS_CONN_CONF['schema'], targetTableWithoutSchema, rfConn):
            postActionSql = appendDataSql.replace("begin;", "begin; {0}".format(createTableSql))
        
        logger.info(f"execute sql: {postActionSql}")
        
        toSaveDF.write \
                .format("io.github.spark_redshift_community.spark.redshift") \
                .option("url", RS_CONN_CONF["url"]) \
                .option("dbtable", stageTable) \
                .option("user", RS_CONN_CONF["user"]) \
                .option("password", RS_CONN_CONF["password"]) \
                .option("tempdir", tempDir) \
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

#================Main==================
sparkConf = createSparkConf()
sc = SparkContext(conf=sparkConf)
glueCtx = GlueContext(sc)
logger = glueCtx.get_logger()
spark = glueCtx.spark_session
job = Job(glueCtx)
job.init(args['JOB_NAME'], args)

logger.info("PgCDCMSKJob init the job...")

mskDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", MSK_CONF["brokers"]) \
    .option("subscribe",MSK_CONF["topic"]) \
    .option("startingOffsets",MSK_CONF["startingOffsets"]) \
    .option("kafka.consumer.commit.groupid", MSK_CONF["groupId"]) \
    .option("kafkaConsumer.pollTimeoutMs", MSK_CONF["pollTimeoutMs"]) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), CDC_SCHEMA).alias("json")) \
    .select(col("json.*"))

logger.info(f"mskDF schema: {mskDF.schema}")
logger.info(f"mskDF columns: {mskDF.columns}")


logger.info("PgCDCMSKJob make the msk dataframe...")

glueCtx.forEachBatch(frame=mskDF, \
                     batch_function=cdcBatchProcessor, \
                     options=SPARK_BATCH_CONF \
                    )
logger.info("PgCDCMSKJob init the mirco batch processing...")

job.commit()