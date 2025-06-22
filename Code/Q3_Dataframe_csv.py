from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import DoubleType, IntegerType

print("--- DataFrame API Implementation (from CSV) ---")

spark = SparkSession.builder.appName("Query3-DataFrame-CSV").getOrCreate()

# --- Step 1: Read and clean the Population data ---
population_df_csv = spark.read.csv(
    'hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv',
    header=True,
    inferSchema=True
).select(
    col("Zip Code").alias("zip_code"),
    col("Total Population").alias("population"),
    col("Total Households").alias("households")
)

# --- Step 2: Read and clean the Income data ---
# The income column is in text format (e.g., "$123,456") and must be cleaned
income_df_csv = spark.read.csv(
    'hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv',
    header=True,
    inferSchema=True
).select(
    col("Zip Code").alias("zip_code"),
    col("Estimated Median Income").alias("est_median_income")
)

# Remove '$' and ',' characters and cast to a numeric type
income_df_cleaned_csv = income_df_csv.withColumn(
    "median_income",
    regexp_replace(col("est_median_income"), "[$,]", "").cast(DoubleType())
)

# --- Step 3: Join the two DataFrames using zip_code ---
# An inner join ensures we only keep Zip Codes present in both datasets
joined_df_csv = population_df_csv.join(
    income_df_cleaned_csv,
    "zip_code",
    "inner"
)

# --- Step 4: Calculate the income per capita ---
# Handle cases where population might be zero to avoid division-by-zero errors
income_per_capita_csv = joined_df_csv.withColumn(
    "income_per_capita",
    when(col("population") > 0, (col("median_income")*col("households")) / col("population"))
    .otherwise(0)
).select(
    "zip_code",
    "income_per_capita"
)

print("Result from CSV files:")
income_per_capita_csv.show(20)

# Stop Spark session
spark.stop()