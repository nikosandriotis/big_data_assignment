import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, rank, to_timestamp, when, desc, udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("Q1 - Dataframe / Parquet (UDF)").getOrCreate()

# Step 1: Define the Python function
def classify_age_group_py(age):
    if age is None: return "Unknown"
    if age < 18: return "Kids"
    elif 18 <= age <= 24: return "Young Adults"
    elif 25 <= age <= 64: return "Adults"
    elif age > 64: return "Elderly"
    else: return "Unknown"

# Step 2: Create the UDF
age_group_udf = udf(classify_age_group_py, StringType())


# Step 3: Read and union the Parquet files
df1 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
df2 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')
df = df1.union(df2)

# Step 4: Filter nulls and relevant crimes
df = df.dropna(subset=["Crm Cd Desc", "Vict Age"]) \
                               .filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT") & (col("Vict Age") > 0))

# Step 5: Apply the UDF to create the new column
age_groups_df_udf = df.withColumn("Age Group", age_group_udf(col("Vict Age")))

# Step 6: Group, count, and sort
result_udf = age_groups_df_udf.groupBy("Age Group") \
                              .count() \
                              .withColumnRenamed("count", "Aggravated_Incidents_Count") \
                              .orderBy(desc("Aggravated_Incidents_Count"))

result_udf.show()

# Stop Spark session
spark.stop()