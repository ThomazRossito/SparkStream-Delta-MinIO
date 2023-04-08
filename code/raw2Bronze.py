####> Steps <####

## 1 - Step: import Lib functions
## 2 - Step: Spark Session
## 3 - Step: set log level
## 4 - Step: Shuffle Spark
## 5 - Step: Paths
## 6 - Step: Spark Read Strem
## 7 - Step: Dataframe Write Strem Delta - Bronze
## 8 - Step: Optimize - Bronze
## ------------------------------ ##


####>> 1 - Step <<####
from functions import *


####>> Step 2 <<####
spark = session_spark()
print("")
print("conexão com o Spark - ok!!!")
print("Version:", spark.version)
## ------------------------------ ##


####>> Step 3 <<####
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
spark.sparkContext.setLogLevel("ERROR")
## ------------------------------ ##


####>> Step 4 <<####
spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
## ------------------------------ ##


####>> Step 5 <<####
## Source ##
sourceBucket = "raw"
prefixBucket = "user"
sourcePath = f"s3a://{sourceBucket}/{prefixBucket}/"

## Target ##
targetBucket = "bronze"
tables = "tables"
checkpoint = "checkpoint"
bronzePath = f"s3a://{targetBucket}/{tables}/{prefixBucket}/"
bronzeChkp = f"s3a://{targetBucket}/{checkpoint}/{prefixBucket}/"
## ------------------------------ ##


####>> Step 6 <<####
print("")
print("Start of read process on Tier: Raw")

dfRead = read_stream_json(spark, sourcePath, schema_evolution(sourceBucket, "json", prefixBucket))
dfRaw = transformation_rawToBronze(dfRead)

print("End of read process on Tier: Raw")
## ------------------------------ ##


####>> Step 7 <<####
print("")
print("Start Ingest Processing on Tier: Bronze")

namedRawToBronze = "namedRawToBronze"
rawToBronzeWriter = create_stream_writer(
    dataframe=dfRaw,
    checkpoint=bronzeChkp,
    trigger=True,
    name=namedRawToBronze,
    mergeSchema=True,
    partition_column=["year", "month", "day"]
)

# start streaming
qry = rawToBronzeWriter.start(bronzePath)
qry.awaitTermination()

print("End Ingest Processing on Tier: Bronze")
## ------------------------------ ##


####>> Step 8 <<####
optimizePathDelta(spark, bronzePath)
print("")
print("End Optimize on Tier: Bronze")
## ------------------------------ ##


dfBronze = spark.read.format("delta").load(bronzePath)
print("")
print("Ingest Count Bronze", dfBronze.count())
# dfBronze.printSchema()
## ------------------------------ ##


spark.stop()