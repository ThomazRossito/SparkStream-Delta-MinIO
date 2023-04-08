####> Functions <####

## 1 - import Libs
## 2 - Functions: session_spark
## 3 - Functions: Schema Evolution
## 4 - Functions: Read Stream format Json
## 5 - Functions: Write Stream format Delta
## 6 - Functions: Tranformation rawToBronze
## 7 - Functions: Optimize Path Delta
## ------------------------------ ##


####>> Item 1 <<####
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter
from minio import Minio
from delta.tables import *
import os
## ------------------------------ ##


####>> Item 2 <<####
def session_spark():
    spark = (
        SparkSession.builder
            .master("local[*]")
            .appName("appMinIO")
            ## Config Fields
            .config('spark.sql.debug.maxToStringFields', 5000)
            .config('spark.debug.maxToStringFields', 5000)
            ## Optimize
            .config("delta.autoOptimize.optimizeWrite", "true")
            .config("delta.autoOptimize.autoCompact", "true")
            ## Delta Table
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            ## MinIO
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            ## Jars
            .config("spark.jars", "/Users/thomaz_rossito/driver_connection/jars/aws-java-sdk-bundle-1.11.1026.jar, \
                                   /Users/thomaz_rossito/driver_connection/jars/hadoop-aws-3.3.2.jar")
            .config("spark.driver.extraClassPath", \
                                  "/Users/thomaz_rossito/driver_connection/jars/mysql-connector-j-8.0.32.jar")
            ## Hive SQL
            .enableHiveSupport()
            .getOrCreate()
    )
    return spark
## ------------------------------ ##


####>> Item 3 <<####
def schema_evolution(pathEvolution: str, typeFile: str, prefixEvolution: str):
    spark = session_spark()
    ## Obter file mais recente ##
    try:
        client = Minio(
            "localhost:9000",
            access_key=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            secure=False
        )
        ## List file in bucket
        objects = client.list_objects(f"{pathEvolution}", recursive=True)
        files = []
        for obj in objects:
            files.append({
                "path": obj.object_name
            })

        sparkDF = spark.createDataFrame(files)
        pathMax = sparkDF.select(regexp_replace(max("path"), "user/", "").alias("path")).collect()[0][0]

        fileDF = spark.read.format(typeFile).load(f"s3a://{pathEvolution}/{prefixEvolution}/{pathMax}").schema
        schemaJson = fileDF.json()
        schemaDDL = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schemaJson).toDDL()
    except:
        print("Directory does not exist, exiting processing!")
        sys.exit(1)
    return schemaDDL
## ------------------------------ ##


####>> Item 4 <<####
def read_stream_json(spark: SparkSession, path: str, schema: str) -> DataFrame:
    return spark.readStream.format("json").schema(schema).load(path)
## ------------------------------ ##


####>> Item 5 <<####
def create_stream_writer(
        dataframe: DataFrame,
        checkpoint: str,
        name: str,
        partition_column: str = None,
        trigger: bool = True,
        mode: str = "append",
        mergeSchema: bool = False,
        ignoreDeletes: bool = False,
        foreach_batch: str = None,
        processingTime: str = None

) -> DataStreamWriter:
    stream_writer = (
        dataframe.writeStream.format("delta")
        .outputMode(mode)
        .option("checkpointLocation", checkpoint)
        .queryName(name)
    )

    if mergeSchema:
        stream_writer = stream_writer.option("mergeSchema", True)
    if ignoreDeletes:
        stream_writer = stream_writer.option("ignoreDeletes", True)
    if trigger:
        stream_writer = stream_writer.trigger(availableNow=True)
    if partition_column is not None:
        stream_writer = stream_writer.partitionBy(partition_column)
    if foreach_batch is not None:
        stream_writer = stream_writer.foreachBatch(foreach_batch)
    if processingTime is not None:
        stream_writer = stream_writer.trigger(processingTime=processingTime)
    return stream_writer
## ------------------------------ ##


####>> Item 6 <<####
def transformation_rawToBronze(df) -> DataFrame:
    dfTranf = (df.withColumn("dt_load", current_date())
                 .withColumn("year", year("dt_load"))
                 .withColumn("month", month("dt_load"))
                 .withColumn("day", dayofmonth("dt_load"))
                 .withColumn("file_name", input_file_name()))
    return dfTranf
## ------------------------------ ##


####>> Item 7 <<####
def optimizePathDelta(spark: SparkSession, path: str):
    deltaTable = DeltaTable.forPath(spark, path)
    deltaTable.optimize()