'''
 reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
'''

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, regexp_extract_all
from pyspark.sql.types import StringType

absPath = os.path.abspath(__file__)
filesFolderPath = os.path.join(os.path.dirname(absPath), 'files')

spark = SparkSession \
        .builder \
        .appName('StructuredStreamingExample') \
        .getOrCreate()

file = spark \
        .readStream \
        .format("text") \
        .option('path', filesFolderPath) \
        .load()

@udf(returnType=StringType())
def processFile(fileContent):
    
    return fileContent.upper()

processedDataframe = file.select(processFile(col('value'))).alias('processedFile')

query = processedDataframe \
          .writeStream \
          .format('console') \
          .start()

query.awaitTermination()
