from pyspark.sql import SparkSession

print("--- Spark SQL API Implementation for Query 2 ---")

# Create Spark Session
spark = SparkSession.builder.appName("Query2-SQL").getOrCreate()

# --- Step 1: Load data and create temporary views ---

# Load the two crime datasets
df1 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2010_2019.parquet')
df2 = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Crime_Data_2020_2025.parquet')

# Combine them using the .union() method as requested
crimes_df = df1.union(df2)

# Load the police station dataset
stations_df = spark.read.parquet('hdfs://hdfs-namenode:9000/user/nandriotis/data/parquet/LA_Police_Stations.parquet')

# Register the DataFrames as temporary SQL views so we can query them
crimes_df.createOrReplaceTempView("crimes")
stations_df.createOrReplaceTempView("stations")

# --- Step 2: Define and execute the SQL query ---

# We use Common Table Expressions (CTEs) with the WITH clause to build the query step-by-step
# This makes the complex logic easier to read and understand.
sql_query = """
    WITH 
    -- Step 2a: Calculate total and closed cases for each precinct per year
    case_stats AS (
        SELECT
            -- Note: We use backticks (`) for column names with spaces
            YEAR(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS year, 
            `AREA ` AS area,
            COUNT(*) AS total_cases,
            SUM(CASE WHEN `Status Desc` NOT IN ('UNK', 'Invest Cont') THEN 1 ELSE 0 END) AS closed_cases
        FROM crimes
        WHERE TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a') IS NOT NULL AND `AREA ` IS NOT NULL
        GROUP BY year, area
    ),
    -- Step 2b: Calculate the closed case rate and rank each precinct within its year
    ranked_stats AS (
        SELECT
            year,
            area,
            (closed_cases / total_cases) * 100 AS closed_case_rate,
            -- The RANK() window function partitions the data by year and orders it
            RANK() OVER (PARTITION BY year ORDER BY (closed_cases / total_cases) DESC) as ranking
        FROM case_stats
    )
    -- Step 2c: Final selection of columns, joining to get precinct name, and filtering for top 3
    SELECT
        rs.year,
        st.DIVISION AS precinct,
        rs.closed_case_rate,
        rs.ranking AS `#`
    FROM ranked_stats rs
    JOIN stations st ON rs.area = st.PREC
    WHERE rs.ranking <= 3
    ORDER BY rs.year, `#`
"""

# --- Step 3: Execute the query and show the results ---
result_sql_df = spark.sql(sql_query)

result_sql_df.show(48)

# Stop the Spark session
spark.stop()