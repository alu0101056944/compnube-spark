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
from pyspark.sql.functions import udf, col, regexp_extract_all, lit
from pyspark.sql.types import StringType

absPath = os.path.abspath(__file__)
filesFolderPath = os.path.join(os.path.dirname(absPath), 'files')

spark = SparkSession \
        .builder \
        .appName('StructuredStreamingExample') \
        .getOrCreate()

file = spark \
        .readStream \
        .format('text') \
        .option('wholeText', 'true') \
        .option('path', filesFolderPath) \
        .load()

boneRegExp = r"(?m)((A|AN)\s+\w+\s+BONE\s+OF\s+MEASUREMENTS\s+(\w+\s+=\s+(\d*\.?\d+)\s+)+)"
allBoneText = file.select(regexp_extract_all('value', lit(boneRegExp)).alias('boneText'))

def process_batch(df, epoch_id):
    # Collect the results
    results = df.collect()
    
    print(f"\nBatch {epoch_id}:")
    for row in results:
        print("\nBone entries:")
        for bone_text in row['boneText']:
            print(bone_text)
            print("---")  # Separator between bone entries

query = allBoneText \
          .writeStream \
          .foreachBatch(process_batch) \
          .start()

query.awaitTermination()
