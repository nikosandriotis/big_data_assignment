from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import DoubleType

print("--- Corrected DataFrame API Implementation (from Parquet) ---")

spark = SparkSession.builder.appName("Query3-DataFrame-Parquet-Corrected").getOrCreate()

# --- Step 1: Read and CLEAN the Population data from Parquet ---
population_df = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/2010_Census_Populations_by_Zip_Code.parquet').select(
    col("Zip Code").alias("zip_code"),
    col("Total Population").alias("population"),
    col("Total Households").alias("households")
)

# --- Step 2: Read and CLEAN the Income data from Parquet ---
# We apply the same cleaning logic to remove '$' and ',' and cast the type.
income_df_raw = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_income_2015.parquet').select(
    col("Zip Code").alias("zip_code"),
    col("Estimated Median Income").alias("median_income_str")
)

income_df = income_df_raw.withColumn(
    "median_income",
    regexp_replace(col("median_income_str"), "[$,]", "").cast(DoubleType())
)

# --- Step 3: Join the two cleaned DataFrames ---
joined_df = population_df.join(
    income_df,
    "zip_code",
    "inner"
)

# --- Step 4: Calculate the income per capita with the correct formula ---
income_per_capita = joined_df.withColumn(
    "income_per_capita",
    when(col("population") > 0,
         (col("median_income") * col("households")) / col("population")
    ).otherwise(0)
).select(
    "zip_code",
    "income_per_capita"
)

print("Result from Parquet files with corrected cleaning logic:")
income_per_capita.show(20)

spark.stop()