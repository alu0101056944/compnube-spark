'''
 reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

 Expected input:
 A file that looks like this:

COMPARE THE RADIOGRAPHY OBSERVED BY Ibrahim
  USING THE realAtlas ATLAS
  STARTING WITH GENDER male
  DEFINED BY
    A Radius BONE OF MEASUREMENTS
    length = 7.2
    width = 16
    
    AN Ulna BONE OF MEASUREMENTS
    length = 5.1
    width = 8.4

    A Metacarpal BONE OF MEASUREMENTS
    length = 3.7
    widthEpiphysis2 = 9.5
    widthEpiphysis3 = 10.134
    widthEpiphysis4 = 8.394

    AN Intersesamoids BONE OF MEASUREMENTS
    distance = 1.2


That represents a radiography with bones and its measurements.
    And its compared against an atlas to get it's bone age. 
        
'''

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract_all, lit, split, base64, \
                                  element_at
from pyspark.sql.types import StringType, StructType, StructField, BinaryType, \
                              TimestampType, LongType

absPath = os.path.abspath(__file__)
filesFolderPath = os.path.join(os.path.dirname(absPath), 'files')

spark = SparkSession \
        .builder \
        .appName('StructuredStreamingExample') \
        .getOrCreate()

schema = StructType([
    StructField("path", StringType(), True), # True means that it can be null
    StructField("modificationTime", TimestampType(), True),
    StructField("length", LongType(), True),
    StructField("content", BinaryType(), True)
])

file = spark \
        .readStream \
        .format('binaryFile') \
        .schema(schema) \
        .load(filesFolderPath)

boneRegExp = r"(?m)((A|AN)\s+\w+\s+BONE\s+OF\s+MEASUREMENTS\s+(\w+\s+=\s+(\d*\.?\d+)\s+)+)"
allBoneText = file.select(
    element_at(split('path', r'[\\/]'), -1),
    regexp_extract_all(
      col('content').cast(StringType()),
      lit(boneRegExp)
    ).alias('boneText')
)

query = allBoneText \
          .writeStream \
          .format('console') \
          .option("truncate", "false")  \
          .start()

query.awaitTermination()
