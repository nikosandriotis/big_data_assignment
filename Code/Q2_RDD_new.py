import time
from pyspark.sql import SparkSession
from datetime import datetime

# --- Helper function to parse and map each row ---
# This contains the logic from Step 3 of the DataFrame script
def parse_and_map_crime_row(row):
    """
    Parses a crime data row, extracts the year, area, and case status.
    Returns a key-value pair for aggregation or None if parsing fails.
    """
    try:
        # Replicate: year(to_timestamp(col('DATE OCC'), 'MM/dd/yyyy hh:mm:ss a'))
        date_occ_str = row['DATE OCC']
        # The Python format string for 'MM/dd/yyyy hh:mm:ss a' is '%m/%d/%Y %I:%M:%S %p'
        occ_datetime = datetime.strptime(date_occ_str, '%m/%d/%Y %I:%M:%S %p')
        year = occ_datetime.year
        
        # Get the area ID, using the exact column name with the trailing space
        area = row['AREA ']
        
        # Replicate: when(col("Status Desc").isin("UNK", "Invest Cont"), 0).otherwise(1)
        is_closed = 0 if row['Status Desc'] in ['UNK', 'Invest Cont'] else 1
        
        # The key is (year, area) for grouping.
        # The value is (is_closed_count, total_count)
        return ((year, area), (is_closed, 1))

    except (TypeError, ValueError, KeyError):
        # Return None if there's an error with date parsing or missing columns
        return None

# --- Main Script ---
print("--- RDD API Implementation of Query 2 ---")

spark = SparkSession.builder.appName("Query2-RDD").getOrCreate()

# --- Step 1 & 2: Read data and union RDDs ---
df1 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
df2 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')
crimes_rdd = df1.union(df2).rdd

# For the join, we collect the small stations data and broadcast it for efficiency
stations_df = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Police_Stations.parquet')
# The join is on 'PREC' and the name is in 'DIVISION'
stations_map = stations_df.rdd.map(lambda row: (row["PREC"], row["DIVISION"])).collectAsMap()
broadcast_stations = spark.sparkContext.broadcast(stations_map)


# --- Step 3 & 4: Map data and aggregate stats ---
# Use the helper function and filter out any rows that failed parsing
stats_rdd = crimes_rdd.map(parse_and_map_crime_row) \
                      .filter(lambda x: x is not None) \
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
# Resulting RDD: ( (year, area), (total_closed, total_cases) )


# --- Step 5 & 6 & 7: Calculate rate, rank, and get top 3 per year ---
# First, re-key the RDD by year
rates_by_year_rdd = stats_rdd.map(lambda row: (
    row[0][0], # year is the new key
    (row[0][1], (row[1][0] / row[1][1]) * 100) # value is (area, rate)
)).groupByKey()

# Next, process each year's group to get the top 3 ranked results
def get_top3_ranked(row):
    year, records = row
    # Sort the list of (area, rate) tuples by the rate in descending order
    sorted_records = sorted(list(records), key=lambda x: x[1], reverse=True)
    # Get the top 3
    top3 = sorted_records[:3]
    
    final_results = []
    # Enumerate to get the rank (1, 2, 3)
    for i, record in enumerate(top3):
        area, rate = record
        rank = i + 1
        # Use the broadcast variable to get the precinct name
        precinct_name = broadcast_stations.value.get(area, "Unknown")
        final_results.append((year, precinct_name, rate, rank))
    return final_results

final_rdd = rates_by_year_rdd.flatMap(get_top3_ranked)


# --- Step 8: Final sort and display ---
# Sort by year (ascending) and then by rank (ascending)
final_sorted_list = final_rdd.sortBy(lambda x: (x[0], x[3])).collect()

print("year      precinct             closed_case_rate          #")
for year, precinct, rate, rank_val in final_sorted_list:
    print(f"{year:<10}{precinct:<20}{rate:<25}{rank_val:<5}")

spark.stop()