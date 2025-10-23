"""
COMPLETE PYSPARK LEARNING TEMPLATE FOR INTERVIEWS
==================================================
This template covers all essential PySpark concepts with practical examples.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# ============================================================================
# 1. SPARK SESSION INITIALIZATION
# ============================================================================

spark = SparkSession.builder \
    .appName("PySpark Learning Template") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# ============================================================================
# 2. DATA CREATION - Multiple Methods
# ============================================================================

# Method 1: From list of tuples
data = [
    (1, "John", "Sales", 5000, "2020-01-15"),
    (2, "Alice", "Engineering", 7000, "2019-03-20"),
    (3, "Bob", "Sales", 5500, "2021-06-10"),
    (4, "Charlie", "Engineering", 8000, "2018-11-25"),
    (5, "David", "HR", 4500, "2022-02-14")
]
df = spark.createDataFrame(data, ["id", "name", "department", "salary", "join_date"])

# Method 2: With explicit schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("join_date", StringType(), True)
])
df = spark.createDataFrame(data, schema)

# Method 3: From CSV
# df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)

# Method 4: From JSON
# df = spark.read.json("path/to/file.json")

# Method 5: From Parquet
# df = spark.read.parquet("path/to/file.parquet")

# ============================================================================
# 3. BASIC DATAFRAME OPERATIONS
# ============================================================================

# Show data
df.show()
df.show(5, truncate=False)

# Schema and info
df.printSchema()
df.dtypes
df.columns
df.count()

# Summary statistics
df.describe().show()
df.summary().show()

# Select columns
df.select("name", "department").show()
df.select(df.name, df.department).show()
df.select(col("name"), col("salary")).show()

# Filter/Where
df.filter(df.salary > 5000).show()
df.where(col("department") == "Sales").show()
df.filter((df.salary > 5000) & (df.department == "Sales")).show()

# Distinct and Drop Duplicates
df.select("department").distinct().show()
df.dropDuplicates(["department"]).show()

# Sort/OrderBy
df.orderBy("salary").show()
df.orderBy(col("salary").desc()).show()
df.sort(desc("salary"), asc("name")).show()

# Limit
df.limit(3).show()

# ============================================================================
# 4. COLUMN OPERATIONS
# ============================================================================

# Add new columns
df_with_bonus = df.withColumn("bonus", col("salary") * 0.1)
df_with_bonus = df.withColumn("total_comp", col("salary") + col("bonus"))

# Rename columns
df_renamed = df.withColumnRenamed("name", "employee_name")

# Drop columns
df_dropped = df.drop("join_date")

# Cast column types
df_typed = df.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))
df_typed = df.withColumn("salary", col("salary").cast("double"))

# Multiple column operations
df_transformed = df \
    .withColumn("salary_k", round(col("salary") / 1000, 2)) \
    .withColumn("dept_upper", upper(col("department"))) \
    .withColumn("name_length", length(col("name")))

# ============================================================================
# 5. STRING FUNCTIONS
# ============================================================================

df_strings = df.select(
    col("name"),
    upper(col("name")).alias("upper_name"),
    lower(col("name")).alias("lower_name"),
    length(col("name")).alias("name_length"),
    substring(col("name"), 1, 3).alias("first_3_chars"),
    concat(col("name"), lit(" - "), col("department")).alias("concat_result"),
    trim(col("name")).alias("trimmed"),
    regexp_replace(col("name"), "o", "0").alias("replaced")
)

# ============================================================================
# 6. NUMERIC FUNCTIONS
# ============================================================================

df_numeric = df.select(
    col("salary"),
    round(col("salary") / 1000, 2).alias("salary_k"),
    ceil(col("salary") / 1000).alias("salary_ceil"),
    floor(col("salary") / 1000).alias("salary_floor"),
    abs(col("salary") - 6000).alias("abs_diff"),
    (col("salary") * 0.1).alias("bonus")
)

# ============================================================================
# 7. DATE AND TIME FUNCTIONS
# ============================================================================

df_dates = df.withColumn("join_date", to_date(col("join_date"), "yyyy-MM-dd"))

df_dates = df_dates.select(
    col("name"),
    col("join_date"),
    year(col("join_date")).alias("year"),
    month(col("join_date")).alias("month"),
    dayofmonth(col("join_date")).alias("day"),
    dayofweek(col("join_date")).alias("day_of_week"),
    current_date().alias("today"),
    datediff(current_date(), col("join_date")).alias("days_employed"),
    date_add(col("join_date"), 365).alias("one_year_later"),
    date_format(col("join_date"), "yyyy-MM").alias("year_month")
)

# ============================================================================
# 8. CONDITIONAL OPERATIONS
# ============================================================================

# when() and otherwise()
df_conditional = df.withColumn(
    "salary_category",
    when(col("salary") >= 7000, "High")
    .when(col("salary") >= 5000, "Medium")
    .otherwise("Low")
)

# Multiple conditions
df_conditional = df.withColumn(
    "performance",
    when((col("salary") > 6000) & (col("department") == "Engineering"), "Top Performer")
    .when(col("salary") > 5000, "Good Performer")
    .otherwise("Average")
)

# ============================================================================
# 9. NULL HANDLING
# ============================================================================

# Check for nulls
df.filter(col("salary").isNull()).show()
df.filter(col("salary").isNotNull()).show()

# Drop rows with nulls
df.na.drop().show()  # Drop any row with null
df.na.drop(subset=["salary"]).show()  # Drop rows where salary is null
df.na.drop(thresh=2).show()  # Drop rows with less than 2 non-null values

# Fill nulls
df.na.fill(0).show()  # Fill all nulls with 0
df.na.fill({"salary": 0, "name": "Unknown"}).show()
df.fillna({"salary": df.agg({"salary": "mean"}).collect()[0][0]}).show()

# Replace values
df.na.replace(["Sales"], ["Sales Department"], subset=["department"]).show()

# ============================================================================
# 10. AGGREGATIONS
# ============================================================================

# Basic aggregations
df.agg(
    count("*").alias("total_count"),
    sum("salary").alias("total_salary"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    stddev("salary").alias("stddev_salary")
).show()

# GroupBy aggregations
df.groupBy("department").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary")
).show()

# Multiple group by columns
df.groupBy("department", "name").agg(sum("salary")).show()

# Using agg with dict
df.groupBy("department").agg({"salary": "mean", "id": "count"}).show()

# Having clause (filter after groupBy)
df.groupBy("department") \
    .agg(avg("salary").alias("avg_salary")) \
    .filter(col("avg_salary") > 5000) \
    .show()

# ============================================================================
# 11. WINDOW FUNCTIONS
# ============================================================================

# Define windows
window_dept = Window.partitionBy("department").orderBy("salary")
window_all = Window.orderBy("salary")

df_window = df.withColumn("row_num", row_number().over(window_dept)) \
    .withColumn("rank", rank().over(window_dept)) \
    .withColumn("dense_rank", dense_rank().over(window_dept)) \
    .withColumn("percent_rank", percent_rank().over(window_dept)) \
    .withColumn("ntile", ntile(3).over(window_dept))

# Aggregate window functions
df_window_agg = df.withColumn("dept_avg_salary", avg("salary").over(Window.partitionBy("department"))) \
    .withColumn("dept_max_salary", max("salary").over(Window.partitionBy("department"))) \
    .withColumn("dept_total_salary", sum("salary").over(Window.partitionBy("department"))) \
    .withColumn("running_total", sum("salary").over(window_dept))

# Lead and Lag
df_lead_lag = df.withColumn("next_salary", lead("salary", 1).over(window_dept)) \
    .withColumn("prev_salary", lag("salary", 1).over(window_dept))

# First and Last
df_first_last = df.withColumn("first_in_dept", first("salary").over(window_dept)) \
    .withColumn("last_in_dept", last("salary").over(window_dept))

# ============================================================================
# 12. JOINS
# ============================================================================

# Create second dataframe for joins
dept_data = [
    ("Sales", "New York"),
    ("Engineering", "San Francisco"),
    ("HR", "Chicago")
]
dept_df = spark.createDataFrame(dept_data, ["department", "location"])

# Inner Join (default)
df.join(dept_df, "department").show()
df.join(dept_df, df.department == dept_df.department, "inner").show()

# Left Join
df.join(dept_df, "department", "left").show()

# Right Join
df.join(dept_df, "department", "right").show()

# Full Outer Join
df.join(dept_df, "department", "outer").show()

# Left Semi Join (like inner join but returns only left columns)
df.join(dept_df, "department", "left_semi").show()

# Left Anti Join (returns rows from left with no match in right)
df.join(dept_df, "department", "left_anti").show()

# Cross Join (Cartesian product)
df.crossJoin(dept_df).show()

# ============================================================================
# 13. UNIONS AND INTERSECTIONS
# ============================================================================

df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
df2 = spark.createDataFrame([(2, "B"), (3, "C")], ["id", "value"])

# Union (includes duplicates)
df1.union(df2).show()

# Union without duplicates
df1.union(df2).distinct().show()

# Intersection
df1.intersect(df2).show()

# Except (rows in df1 but not in df2)
df1.exceptAll(df2).show()

# ============================================================================
# 14. PIVOTING AND UNPIVOTING
# ============================================================================

# Pivot
pivot_df = df.groupBy("department").pivot("name").agg(sum("salary"))
pivot_df.show()

# Unpivot (using stack)
unpivot_df = pivot_df.select(
    col("department"),
    expr("stack(5, 'Alice', Alice, 'Bob', Bob, 'Charlie', Charlie, 'David', David, 'John', John) as (name, salary)")
)

# ============================================================================
# 15. SQL QUERIES
# ============================================================================

# Register as temp view
df.createOrReplaceTempView("employees")

# Run SQL queries
spark.sql("""
    SELECT department, 
           COUNT(*) as emp_count, 
           AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
    HAVING AVG(salary) > 5000
""").show()

# Complex SQL query
spark.sql("""
    SELECT 
        name,
        department,
        salary,
        RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
""").show()

# ============================================================================
# 16. USER DEFINED FUNCTIONS (UDFs)
# ============================================================================

from pyspark.sql.types import IntegerType

# Python UDF
def categorize_salary(salary):
    if salary >= 7000:
        return "High"
    elif salary >= 5000:
        return "Medium"
    else:
        return "Low"

# Register UDF
categorize_udf = udf(categorize_salary, StringType())

# Use UDF
df.withColumn("category", categorize_udf(col("salary"))).show()

# Pandas UDF (faster for vectorized operations)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(StringType())
def categorize_pandas_udf(salary: pd.Series) -> pd.Series:
    return salary.apply(lambda x: "High" if x >= 7000 else "Medium" if x >= 5000 else "Low")

df.withColumn("category", categorize_pandas_udf(col("salary"))).show()

# ============================================================================
# 17. HANDLING MISSING DATA AND OUTLIERS
# ============================================================================

# Detect nulls
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Imputation with mean
mean_salary = df.select(mean(col("salary"))).collect()[0][0]
df.fillna({"salary": mean_salary}).show()

# Outlier detection using IQR
quantiles = df.approxQuantile("salary", [0.25, 0.75], 0.05)
Q1, Q3 = quantiles[0], quantiles[1]
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

df_no_outliers = df.filter((col("salary") >= lower_bound) & (col("salary") <= upper_bound))

# ============================================================================
# 18. REPARTITIONING AND CACHING
# ============================================================================

# Repartition
df_repartitioned = df.repartition(4)
df_repartitioned = df.repartition(4, "department")

# Coalesce (reduce partitions)
df_coalesced = df.coalesce(2)

# Cache/Persist
df.cache()
df.persist()
df.unpersist()

# Check partitions
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

# ============================================================================
# 19. WRITING DATA
# ============================================================================

# Write to CSV
# df.write.csv("output/csv", header=True, mode="overwrite")

# Write to Parquet
# df.write.parquet("output/parquet", mode="overwrite")

# Write to JSON
# df.write.json("output/json", mode="overwrite")

# Partitioned write
# df.write.partitionBy("department").parquet("output/partitioned")

# Write to table
# df.write.saveAsTable("employee_table", mode="overwrite")

# ============================================================================
# 20. COMMON INTERVIEW PATTERNS
# ============================================================================

# Find top N per group
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
top_earners = df.withColumn("rank", rank().over(window_spec)) \
    .filter(col("rank") <= 2) \
    .drop("rank")

# Running totals
running_total_window = Window.partitionBy("department").orderBy("join_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_running = df.withColumn("running_total", sum("salary").over(running_total_window))

# Moving average
moving_avg_window = Window.orderBy("join_date").rowsBetween(-2, 0)
df_moving_avg = df.withColumn("moving_avg", avg("salary").over(moving_avg_window))

# Find duplicates
df.groupBy("name").count().filter(col("count") > 1).show()

# Cumulative percentage
total_salary = df.agg(sum("salary")).collect()[0][0]
df.withColumn("percentage", (col("salary") / total_salary * 100)).show()

# Self join (find employees in same department)
df_self = df.alias("df1").join(
    df.alias("df2"),
    (col("df1.department") == col("df2.department")) & (col("df1.id") != col("df2.id"))
).select(col("df1.name").alias("employee1"), col("df2.name").alias("employee2"), col("df1.department"))

print("\n=== PySpark Learning Template Complete ===")
print("Practice these concepts and modify the examples!")
print("Remember to stop the Spark session when done:")
# spark.stop()