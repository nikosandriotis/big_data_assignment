from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, year, when, count, sum, desc, rank, to_timestamp
from pyspark.sql.window import Window

print("--- DataFrame API Implementation using .union() ---")

spark = SparkSession.builder.appName("Query2-DataFrame").getOrCreate()

# --- Step 1: Read the necessary Parquet files ---
df1 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
df2 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')
stations_df = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Police_Stations.parquet')

# --- Step 2: Combine the two crime DataFrames using union() ---
crimes_df = df1.union(df2)

# --- Step 3: Prepare the data ---
# Create 'year' and 'is_closed' columns
processed_crimes_df = crimes_df.withColumn("year", year(to_timestamp(col('DATE OCC'), 'MM/dd/yyyy hh:mm:ss a'))) \
                               .withColumn("is_closed", 
                                           when(col("Status Desc").isin("UNK", "Invest Cont"), 0)
                                           .otherwise(1))

# --- Step 4: Calculate stats per year and police area ---
area_stats_df = processed_crimes_df.groupBy("year", "AREA ") \
                                   .agg(
                                       count("*").alias("total_cases"),
                                       sum("is_closed").alias("closed_cases")
                                   )

# --- Step 5: Calculate the closed case rate ---
rates_df = area_stats_df.withColumn("closed_case_rate", (col("closed_cases") / col("total_cases")) * 100)

# --- Step 6: Rank precincts within each year using a window function ---
windowSpec = Window.partitionBy("year").orderBy(desc("closed_case_rate"))
ranked_df = rates_df.withColumn("ranking", rank().over(windowSpec))

# --- Step 7: Filter for the top 3 precincts of each year ---
top3_df = ranked_df.filter(col("ranking") <= 3)

# --- Step 8: Join with station data to get names and format the final output ---
final_df = top3_df.join(broadcast(stations_df), top3_df["AREA "] == stations_df["PREC"]) \
                  .select(
                      top3_df.year,
                      stations_df.DIVISION.alias("precinct"),
                      top3_df.closed_case_rate,
                      top3_df.ranking.alias("#")
                  ) \
                  .orderBy("year", "#")

final_df.show(48)

# Stop Spark session
spark.stop()