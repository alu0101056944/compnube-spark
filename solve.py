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
from pyspark.sql.functions import col, regexp_extract_all, lit, split, explode, \
                                  element_at, regexp_extract, abs, min, filter
from pyspark.sql.types import StringType, StructType, StructField, BinaryType, \
                              TimestampType, LongType, FloatType

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


# Prepare the atlas file

atlasFile = spark.read.text('atlas.ae', wholetext=True)

atlasRadiographyRegExp = r"(?m)(ONE\s+FOR\s+GENDER\s+(\w+)\s+AGE\s+(\d*\.?\d+)\s+WITH\s+(((A|AN)\s+)?(\w+)\s+BONE\s+OF\s+MEASUREMENTS\s+((\w+)\s+=\s+(\d*\.?\d+)\s+)+)+)"
withArrayOfRadiographies = atlasFile.select(
  regexp_extract_all(
    col('value'),
    lit(atlasRadiographyRegExp)
  ).alias('radiographyTexts')
)
oneRowPerAtlasRadiography = withArrayOfRadiographies.select(
  explode(col('radiographyTexts')).alias('radiographyText')
)

atlasGenderAndAgeRegExp = r"(?m)ONE\s+FOR\s+GENDER\s+(\w+)\s+AGE\s+(\d*\.?\d+)\s+WITH"
atlasBoneRegExp = r"(?m)(((A|AN)\s+)?(\w+)\s+BONE\s+OF\s+MEASUREMENTS\s+((\w+)\s+=\s+(\d*\.?\d+)\s+)+)"
withAgeAndGenderAndAtlasBoneText = oneRowPerAtlasRadiography.select(
  regexp_extract(
    col('radiographyText'),
    atlasGenderAndAgeRegExp,
    1
  ).alias('gender'),
  regexp_extract(
    col('radiographyText'),
    atlasGenderAndAgeRegExp,
    2
  ).alias('age').cast(FloatType()),
  regexp_extract_all(
    col('radiographyText'),
    lit(atlasBoneRegExp),
    1
  ).alias('boneTexts'),
)

oneAtlasBoneTextPerRow = withAgeAndGenderAndAtlasBoneText.select(
  col('gender'),
  col('age'),
  explode(col('boneTexts')).alias('boneText')
)

atlasBoneNameRegExp = r"(?m)((A|AN)\s+)?(\w+)\s+BONE\s+OF\s+MEASUREMENTS"
atlasBoneMeasurementRegExp = r"(?m)((\w+)\s+=\s+\d*\.?\d+)"
oneAtlasBonePerRowWithMeasurementTexts = oneAtlasBoneTextPerRow.select(
  col('gender'),
  col('age'),
  regexp_extract(
    col('boneText'),
    atlasBoneNameRegExp,
    3
  ).alias('bone'),
  regexp_extract_all(
    col('boneText'),
    lit(atlasBoneMeasurementRegExp),
    1
  ).alias('measurementTexts'),
)

oneRowPerAtlasBoneMeasurementText = oneAtlasBonePerRowWithMeasurementTexts.select(
  col('gender'),
  col('age'),
  col('bone'),
  explode(col('measurementTexts')).alias('measurementText')
)

atlasBoneMeasurementsRegExp = r"(?m)(\w+)\s+=\s+(\d*\.?\d+)"
oneRowPerAtlasBoneMeasurement = oneRowPerAtlasBoneMeasurementText.select(
  col('gender'),
  col('age'),
  col('bone'),
  regexp_extract(
    col('measurementText'),
    atlasBoneMeasurementsRegExp,
    1
  ).alias('measurement_name'),
    regexp_extract(
    col('measurementText'),
    atlasBoneMeasurementsRegExp,
    2
  ).alias('measurement_value').cast(FloatType()),
)


# Stream File processing

radiographyFile = spark \
        .readStream \
        .format('binaryFile') \
        .schema(schema) \
        .load(filesFolderPath)


withFileInformationProcessed = radiographyFile.select(
  element_at(split('path', r'[\\/]'), -1).alias('filename'),
  col('content').cast(StringType()).alias('content')
)

boneRegExp = r"(?m)(((A|AN)\s+)?\w+\s+BONE\s+OF\s+MEASUREMENTS\s+(\w+\s+=\s+(\d*\.?\d+)\s+)+)"
genderRegExp = r"(?m)STARTING\s+WITH\s+GENDER\s+(\w+)"
withSingleArrayOfBoneText = withFileInformationProcessed.select(
  col('filename'),
  regexp_extract(col('content'), genderRegExp, 1).alias('gender'),
  regexp_extract_all(
    col('content'),
    lit(boneRegExp)
  ).alias('boneText')
)
oneRowPerBoneText = withSingleArrayOfBoneText.select(
  col('filename'),
  col('gender'),
  explode(col('boneText')).alias('boneText'),
)

boneNameRegExp = r"(?m)((A|AN)\s+)?(\w+)\s+BONE\s+OF\s+MEASUREMENTS"
boneMeasurementsRegExp = r"(?m)((\w+)\s+=\s+\d*\.?\d+)"
oneRowPerBone = oneRowPerBoneText.select(
  col('filename'),
  col('gender'),
  regexp_extract(col('boneText'), boneNameRegExp, 3).alias('bone'),
  regexp_extract_all(
    col('boneText'),
    lit(boneMeasurementsRegExp)
  ).alias('measurements'),
)

oneRowPerBoneMeasurementText = oneRowPerBone.select(
  col('filename'),
  col('gender'),
  col('bone'),
  explode(col('measurements')).alias('measurements'),
)

measurementNameRegExp = r"(\w+)\s+=\s+\d*\.?\d+"
measurementValueRegExp = r"\w+\s+=\s+(\d*\.?\d+)"
oneRowPerBoneMeasurement = oneRowPerBoneMeasurementText.select(
  col('filename'),
  col('gender'),
  col('bone'),
  regexp_extract(
    col('measurements'),
    measurementNameRegExp,
    1
  ).alias('measurement_name'),
  regexp_extract(
    col('measurements'),
    measurementValueRegExp,
    1
  ).alias('measurement_value').cast(FloatType()),
)

# Calculate the minimum difference atlas radiography against our radiography
def process_batch(batch_df, batch_id):
  radiographyAndAtlasRadiographiesJoin = batch_df.join(
    oneRowPerAtlasBoneMeasurement,
    on=['gender', 'bone', 'measurement_name'],
    how="left"
  )

  withDifferences = radiographyAndAtlasRadiographiesJoin.select(
    oneRowPerBoneMeasurement['filename'],
    oneRowPerBoneMeasurement['gender'],
    oneRowPerBoneMeasurement['bone'],
    oneRowPerBoneMeasurement['measurement_name'],
    oneRowPerBoneMeasurement['measurement_value'],
    oneRowPerAtlasBoneMeasurement['gender'].alias('atlas_gender'),
    oneRowPerAtlasBoneMeasurement['age'].alias('atlas_age'),
    oneRowPerAtlasBoneMeasurement['bone'].alias('atlas_bone'),
    oneRowPerAtlasBoneMeasurement['measurement_name'] \
        .alias('atlas_measurement_name'),
    oneRowPerAtlasBoneMeasurement['measurement_value'] \
        .alias('atlas_measurement_value'),
    abs((oneRowPerAtlasBoneMeasurement['measurement_value'] -
        oneRowPerBoneMeasurement['measurement_value'])) \
          .alias('measurement_values_difference')
  )

  totalDifferences = withDifferences \
    .groupBy(['filename', 'atlas_age']) \
    .sum("measurement_values_difference")
  
  smallestDifferences = totalDifferences \
    .groupBy(['filename']) \
    .min("sum(measurement_values_difference)")
  
  withMinimumDifferences = totalDifferences \
    .join(smallestDifferences, on='filename', how='left')
  
  finalAgesWithUnwantedColumns = withMinimumDifferences \
    .filter(withMinimumDifferences['sum(measurement_values_difference)'] == \
            withMinimumDifferences['min(sum(measurement_values_difference))'])

  finalAges = finalAgesWithUnwantedColumns.select(
    col('filename'),
    col('atlas_age').alias('age')
  )

  finalAges.show(truncate=False)

query = oneRowPerBoneMeasurement \
          .writeStream \
          .foreachBatch(process_batch) \
          .start()

query.awaitTermination()

spark.stop()
