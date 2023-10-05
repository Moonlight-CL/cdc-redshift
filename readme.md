## About
This repo contains two Amazon Glue pyspark scripts that are used to read the cdc data from Amazon MSK and then apply the change data to Amazon Redshift, and we use Redshift SUPER data type as the document / record data type. 
-  `glue_docdb_redshift_cdc.py`: cdc data from Amazon DocumentDB
- `glue_pg_redshift_cdc.py`: cdc data from RDS PostgreSQL

## How to use
1. Create a Glue data connection for Redshift.
2. Create a Glue job, and paste the content of script as the Glue job script, add a job parameter with key name `--additional-python-modules` and value `redshift_connector`, and choose the connection of Redshift.
3. Create a proper IAM Role used for Redshift to `COPY` / `UNLOAD` data from/to s3. Follow the guide [Authorizing Amazon Redshift to Access Other AWS Services On Your Behalf](http://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html) to configure this role's trust policy in order to allow Redshift to assume this role. Follow the steps in the [Authorizing COPY and UNLOAD Operations Using IAM Roles](http://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html) guide to associate that IAM role with your Redshift cluster.
4. Create a json file needed by the Glue job, it contains the job parameters, and then save it as a s3 object. The json file like this:
```json
{
    "redshift_conf": {
        "url": "jdbc:redshift://xxx:5439/db",
        "cluster": "cluster-xxx.region.redshift.amazonaws.com",
        "port":  5439,
        "database": "db",
        "schema": "schema",
        "user": "user",
        "password": "pwd",
        "tmpdir": "s3 path for redshift-spark integration",
        "aws_iam_role": "arn of iam role for redshift"
    },
    "msk_conf": {
        "brokers": "kafka broker list",
        "topic": "kafka topic",
        "maxOffsetsPerTrigger": 200000,
        "groupId": "kafka consumer group id",
        "startingOffsets": "earliest",
        "pollTimeoutMs": 240000
    },
    "spark_batch_conf": {
        "windowSize": "30 seconds", 
        "checkpointLocation": "s3 path for checkpoint"
    },
    "spark_conf": {
        "dynamicAllocationEnabled": "false",
        "executorMemory": "12g",
        "executorCores": 4,
        "shufflePartitions": 1,
        "defaultParallelism": 1,
        "speculation": "fasle"
    }
}
```
5. You can run the job on the AWS Web Console or you can run it by AWS CLI like this:
```shell
aws glue start-job-run \
--job-name YourJobName \
--arguments='{"--S3_CONF_BUCKET": "conf-bucket-name","--S3_CONF_KEY": "path of glue-job.json"}'
```