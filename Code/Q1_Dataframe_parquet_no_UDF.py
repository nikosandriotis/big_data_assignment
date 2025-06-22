import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, rank, to_timestamp, when, desc
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("Q1 - Dataframe / Parquet (noUDF)").getOrCreate()

# Step 1: Read and union the Parquet files
df1 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
df2 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')
df = df1.union(df2)

# Step 2: Filter nulls and relevant crimes
df = df.dropna(subset=["Crm Cd Desc", "Vict Age"]).filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT") & (col("Vict Age") > 0))

# Step 3: Classify age groups using the native when() function
age_groups_df = df.withColumn("Age Group",
     when(col("Vict Age") < 18, "Kids")
    .when(col("Vict Age").between(18, 24), "Young Adults")
    .when(col("Vict Age").between(25, 64), "Adults")
    .when(col("Vict Age") > 64, "Elderly")
    .otherwise("Unknown")
)

# Step 4: Group, count, and sort
result_df = age_groups_df.groupBy("Age Group").count().withColumnRenamed("count", "Aggravated_Incidents_Count").orderBy(desc("Aggravated_Incidents_Count"))

result_df.show()

# Stop Spark session
spark.stop()