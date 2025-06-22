import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, rank, to_timestamp, when, desc, udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# Start measuring execution time
start_timestamp = time.time()

# Create Spark session
spark = SparkSession.builder.appName("Q1 - RDDs / Parquet").getOrCreate()

# Step 1: Read and union the Parquet files
df1 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
df2 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')
df = df1.union(df2)

# The * operator unpacks the list into arguments for the parquet function
crimes_rdd = df.rdd

# Step 2: Filter the records using a Python function
def filter_assaults(row):
    try:
        return "AGGRAVATED ASSAULT" in row["Crm Cd Desc"] and row["Vict Age"] > 0
    except (TypeError, AttributeError):
        return False

filtered_rdd = crimes_rdd.filter(filter_assaults)

# Step 3: Map to key-value pairs using a Python function
def classify_age_group_py(age):
    if age is None: return "Unknown"
    if age < 18: return "Kids"
    elif 18 <= age <= 24: return "Young Adults"
    elif 25 <= age <= 64: return "Adults"
    elif age > 64: return "Elderly"
    else: return "Unknown"

mapped_rdd = filtered_rdd.map(lambda row: (classify_age_group_py(row["Vict Age"]), 1))

# Step 4: Count incidents per group
counts_rdd = mapped_rdd.reduceByKey(lambda a, b: a + b)

# Step 5: Sort by count in descending order
sorted_rdd = counts_rdd.sortBy(lambda x: x[1], ascending=False)


# Step 6: Collect and print results
result_rdd = sorted_rdd.collect()

# Calculate the execution time
end_timestamp = time.time()
execution_time = end_timestamp - start_timestamp
# Print the execution time
print("Execution Time:", execution_time, "seconds")

print("Results from RDD implementation:")
for group, count in result_rdd:
    print(f"Age Group: {group}, Aggravated_Incidents_Count: {count}")

# Stop Spark session
spark.stop()