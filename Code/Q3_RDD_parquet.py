from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DoubleType

print("--- RDD API Implementation (from uncleaned Parquet files) ---")

spark = SparkSession.builder.appName("Query3-RDD-Corrected").getOrCreate()

# --- Phase 1: Data Loading and Cleaning (using DataFrame API) ---

# 1a: Load and clean the Population data
population_df_clean = spark.read.parquet(
    'hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/2010_Census_Populations_by_Zip_Code.parquet'
).select(
    col("Zip Code").alias("zip_code"),
    col("Total Population").alias("population"),
    col("Total Households").alias("households")
)

# 1b: Load and clean the Income data
income_df_raw = spark.read.parquet(
    'hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_income_2015.parquet'
).select(
    col("Zip Code").alias("zip_code"),
    col("Estimated Median Income").alias("median_income_str")
)
income_df_clean = income_df_raw.withColumn(
    "median_income",
    regexp_replace(col("median_income_str"), "[$,]", "").cast(DoubleType())
)

# --- Phase 2: Convert Clean DataFrames to RDDs ---

# Create a key-value RDD: (zip_code, (population, households))
population_rdd = population_df_clean.rdd.map(
    lambda row: (row.zip_code, (row.population, row.households))
)

# Create a key-value RDD: (zip_code, median_income)
income_rdd = income_df_clean.rdd.map(
    lambda row: (row.zip_code, row.median_income)
)

# --- Phase 3: RDD Join and Calculation ---

# 3a: Join the two RDDs on the common key (zip_code)
# The result has the format: (zip_code, ((population, households), median_income))
joined_rdd = population_rdd.join(income_rdd)

# 3b: Define a function to calculate the per-capita income from the joined data
def calculate_per_capita(row):
    zip_code, ((population, households), income) = row
    
    # Check for valid data to avoid errors
    if population is not None and population > 0 and households is not None and income is not None:
        # The correct formula: (Total Income) / Total Population
        return (zip_code, (income * households) / population)
    else:
        return (zip_code, 0.0)

# 3c: Map the joined RDD to get the final calculation
income_per_capita_rdd = joined_rdd.map(calculate_per_capita)

sorted_rdd = income_per_capita_rdd.sortBy(lambda x: x[0], ascending=True)


# --- Phase 4: Final Conversion and Display ---

# Convert the final RDD back to a DataFrame to display it in a readable table
result_df_from_rdd = sorted_rdd.toDF(["zip_code", "income_per_capita"])

print("Result from RDD implementation:")
result_df_from_rdd.show()

spark.stop()