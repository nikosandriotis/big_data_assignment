import time
from pyspark.sql import SparkSession
# Import all necessary SQL functions
from pyspark.sql.functions import col, desc, avg, count, round, lit, sin, cos, split, atan2, sqrt, radians, explode, lower

# Create Spark Session
spark = SparkSession.builder.appName("Q4 - Crime Analysis with Native Haversine").getOrCreate()

# Read Parquet files
df1 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
df2 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')
crime = df1.union(df2) 

police_stations = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Police_Stations.parquet')
mo_codes = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/MO_codes.parquet')

# Step 1: Filter MO codes that mention "gun" or "weapon" (case-insensitive)
mo_codes_filtered = mo_codes.filter(lower(col("Description")).rlike("gun|weapon"))

# Step 2: Prepare crimes with valid mocodes and valid coordinates
crime = crime.filter((col("Mocodes").isNotNull()) & (col("LAT") != 0) & (col("LON") != 0))

# Step 3: Split and explode Mocodes into individual MO_Code values
crime = crime.withColumn("MO_Code", explode(split(col("Mocodes"), " ")))

# Step 4: Join with filtered MO codes
crimes_with_weapons = crime.join(mo_codes_filtered, "MO_Code", "inner")

# Join police stations and crime data
crime_joined_police = crimes_with_weapons.join(
        police_stations, 
        crimes_with_weapons['AREA '] == police_stations["PREC"], 
        "inner"
)

#Calculate distance using Haversine formula with native Spark functions ---
# Earth's radius in kilometers
R = 6371.0

# Convert latitude and longitude from degrees to radians
lat1_rad = radians(col('LAT'))
lon1_rad = radians(col('LON'))
lat2_rad = radians(col('Y'))
lon2_rad = radians(col('X'))

# Calculate the difference in coordinates
dlon = lon2_rad - lon1_rad
dlat = lat2_rad - lat1_rad

# Haversine formula
a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
c = 2 * atan2(sqrt(a), sqrt(1 - a))
distance_expr = lit(R) * c

crime_with_distance = crime_joined_police.withColumn('distance', distance_expr)


# Group by division, count total incidents, and calculate average distance
result_df = crime_with_distance.groupby('division').agg(
    count('*').alias('incidents_total'),
    round(avg('distance'),3).alias('average_distance')
).orderBy(desc('incidents_total'))


# Show the result
result_df.show()

spark.stop()