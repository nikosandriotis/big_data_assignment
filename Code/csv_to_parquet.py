from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
from pyspark.sql.types import IntegerType

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the crimes CSV files into DataFrames
crimes_1 = spark.read.csv('hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv',
                           header = True, inferSchema = True, escape='"') #in case there are quotes inside quotes
crimes_2 = spark.read.csv('hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv',
                           header = True, inferSchema = True, escape='"')
police_stations = spark.read.csv('hdfs://hdfs-namenode:9000/user/root/data/LA_Police_Stations.csv',
                           header = True, inferSchema = True, escape='"')
median_household = spark.read.csv('hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv',
                           header = True, inferSchema = True, escape='"')
pop_by_zip = spark.read.csv('hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv',
                           header = True, inferSchema = True, escape='"')

# Read the text file. This creates a DataFrame with a single column named "value".
mo_codes_raw = spark.read.text('hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt')

# Split the "value" column into two parts: the code and the full description.
split_col = split(col('value'), ' ', 2)

# Create the final DataFrame with two named and correctly typed columns
# .getItem(0) gets the code, .getItem(1) gets the full description.
mo_codes = mo_codes_raw.withColumn('MO_Code', split_col.getItem(0)) \
                       .withColumn('Description', split_col.getItem(1)) \
                       .drop('value') # Drop the original "value" column

 
# Convert DataFrames to Parquet format
crimes_1.write.mode('overwrite').parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
crimes_2.write.mode('overwrite').parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')
police_stations.write.mode('overwrite').parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Police_Stations.parquet')
median_household.write.mode('overwrite').parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_income_2015.parquet')
pop_by_zip.write.mode('overwrite').parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/2010_Census_Populations_by_Zip_Code.parquet')

# Write the new DataFrame to Parquet format, following your existing pattern
mo_codes.write.mode('overwrite').parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/MO_codes.parquet')

# You can optionally show the result to verify
print("MO Codes DataFrame schema and sample:")
mo_codes.printSchema()
mo_codes.show(5, truncate=False)

spark.stop()