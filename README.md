# SPARK FUNDAMENTALS

This is a summary of my studies to the Spark Associate Developer for Apache Spark 3.0, the requirements [are](https://academy.databricks.com/exam/databricks-certified-associate-developer):

- have a basic understanding of the Spark architecture, including Adaptive Query Execution
- be able to apply the Spark DataFrame API to complete individual data manipulation task, including: 
  - selecting, renaming and manipulating columns
  - filtering, dropping, sorting, and aggregating rows
  - joining, reading, writing and partitioning DataFrames
  - working with UDFs and Spark SQL functions
 
## Spark arquitecture

![Spark Architecture](https://github.com/seobando/CERTIFICATIONS_SPARK/blob/main/spark_architecture.jpg?raw=true)

- Spark Application: A user program built on Spark using its APIs. It consists of a driver program and executors on the cluster.

  - Spark Driver:
  
    1. Is the responsible for instantiating a SparkSession. 
    2. Communicates with the cluster manager. 
    3. Request resources (CPU, memory, etc).
    4. Transforms all the Spark operations into DAG computations.
    5. Schedules themand distributes their execution as tasks across the Spark executors. 
    6. Once the resources are allocated. 
    7. It communicates directly with the executor.

   - SparkSession
    
    An object that provides a point of entry to interact with underlying Spark functionality and allows programming Sprk with its APIs. Not only did it subsume previous entry points to Spark like the SparkContext, SQLContext, HiveContext, SparkConf, and StreamingContext, but it also made working with Spark simplear and easier.

- Cluster Manager

Is responsible for managing and allocating resources for the cluster of nodes on which the Spark application runs and Support as cluster managers: the built-in standalone cluster manager, Apache Hadoop, YARN, Apache Mesos, and Kubernetes.

- Spark Executor

  - Runs on each worker node in the cluster. 
  - Communicate with the driver program
  - Are responsible for executing tasks on the workers
  - In most deployments modes, only a single executer runs per node.
  
- Core

  - Job: parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g., save(), collect()). 
 
    - Stage: Each job gets divided into smaller sets of tasks called stages that depend on each other.
  
      - Task: A single unit of work or execution that will be sent to a Spark executor.

## DataFrame API
 
Spark is a distributed programming model in which the user specifies transformations. Multiple transformations build up a directed acyclic graph of instructions. An action begins the process of executing that graph of instructions, as a single job, by breaking it down into stages and tasks to execute across the cluster. The logical structures that we manipulate with transformations and actions are DataFrames and Datasets. To create a new DataFrame or Dataset, you call a transformation. To start computation or convert to native language types, you call an action.

- Transformations examples: orderBy(), groupBy(), filter(), select(), join()
- Actions examples: show(), take(), count(), collect(), save()
 
### Selecting, renaming and manipulating columns:

- Selecting:

```PYTHON
# Form 1
df.select("columnName_1","columnName_2","columnName_3").show()

# Form 2
df.select(df.columnName_1,df.columnName_2,df.columnName_3).show()

# Form 3, 4 and 5
from pyspark.sql.functions import expr, col, column

df.select(expr("COLUMN_NAME"), col("COLUMN_NAME"), column("COLUMN_NAME")).show()

# Select with operation
df.select("columnName or Operation with column").alias("NewColumnName")).show()

# Select with expression Form 1
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME")).show(2)

# Select with expression Form 2
df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)
```

- Renaming:
```PYTHON
df = df.withColumnRenamed("Column", "NewColumnName")
```

- Manipulating columns:
```PYTHON
# Add new columns
df.withColumn("NewColumn", df.columnName[some operation])

# Converting to Spark Types (Literals)
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)
df.select(lit(5), lit("five"), lit(5.0))

# Casting
df.withColumn("count2", col("count").cast("long"))

# Control Flow
df.select(df.Name, df.Age,
  .when(df.Age >= 18, "Adult")
  .otherwise("Minor"))
  
# Getting Unique Rows
df.select("COLUMN_NAME", "COLUMN_NAME").distinct().count()

# Union
df.union(newDF)  
```

> Working with Strings
```PYTHON
# Capitalize every word in a given string
from pyspark.sql.functions import initcap

df.select(initcap(col("COLUMN_NAME"))).show()

# Lowercase or uppercase a string
from pyspark.sql.functions import lower, upper

df.select(col("Description"), lower(col("Description")), upper(lower(col("Description")))).show(2)

# Adding or removing spaces around a string
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim

df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),             # Remove left spaces
rtrim(lit(" HELLO ")).alias("rtrim"),             # Remove right spaces
trim(lit(" HELLO ")).alias("trim"),               # Remove all spaces
lpad(lit("HELLO"), 3, " ").alias("lp"),           # Add and select strings characters from the left 
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)  # Add and select strings characters from the right

+---------+---------+-----+------+
| ltrim| rtrim| trim| lp|      rp|
+---------+---------+---+--------+
|HELLO | HELLO|HELLO| HE|HELLO   |
|HELLO | HELLO|HELLO| HE|HELLO   |
+---------+---------+---+--------+

# Regular Expressions

## Replace or substitute strings
from pyspark.sql.functions import regexp_replace

regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"), col("Description")).show(2)

## Replace given characters with other characters
from pyspark.sql.functions import translate

df.select(translate(col("Description"), "LEET", "1337"),col("Description")).show(2)

## Extract match strings
from pyspark.sql.functions import regexp_extract

extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),col("Description")).show(2)

## Check if string match
from pyspark.sql.functions import instr

containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite).where("hasSimpleColor").select("Description").show(3, False)

## Locate the position of the first ocurrence of substr
from pyspark.sql.functions import expr, locate

simpleColors = ["black", "white", "red", "green", "blue"]

def color_locator(column, color_string):
  return locate(color_string.upper(), column).cast("boolean").alias("is_" + c)

selectedColumns = [color_locator(df.Description, c) for c in simpleColors]

selectedColumns.append(expr("*")) # has to a be Column type

df.select(*selectedColumns).where(expr("is_white OR is_red")).select("Description").show(3, False)
```

> Working with Dates and Timestamps

```PYTHON
# Get current date and current timestamp
from pyspark.sql.functions import current_date, current_timestamp

dateDF = spark.range(10).withColumn("today", current_date()).withColumn("now", current_timestamp())

dateDF.createOrReplaceTempView("dateTable")

# Substract and add days to a date
from pyspark.sql.functions import date_add, date_sub

dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

# Calculate the difference between dates in days
dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(datediff(col("week_ago"), col("today"))).show(1)

# Calculate the difference between dates in months
dateDF.select(to_date(lit("2016-01-01")).alias("start"), \
to_date(lit("2017-05-22")) \ # Allows to convert a string into a date
.alias("end")) \
.select(months_between(col("start"), col("end"))).show(1)

# Convert a string date into date format
from pyspark.sql.functions import to_date, lit

spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).show(1)

# Avoid spark to throws null when don't convert correctly a date by adding the specific format to_date
from pyspark.sql.functions import to_date

## date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")

## timestamp
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
```

> Working with Nulls in Data

```PYTHON
# Select the first nun-null valur from a set of columns
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()

# ifnull, allows to select the second value if the first is null, and defaults to the first
ifnull(null, 'return_value')

# nullif, returns null if the two values are equal or else returns the second if they are not
nullif('value', 'value')

# nvl, returns the second value if the first is null, but defaults to the first
nvl(null, 'return_value')

# nvl2, returns the second value if the first is not null; otherwise, it will return the last specified value
nvl2('not_null', 'return_value', "else_value")

# Drop null
## If any column has a null value
df.na.drop()
df.na.drop("any")

## If all colmns has a null value
df.na.drop("all")

## If specific columns has all null values
df.na.drop("all", subset=["StockCode", "InvoiceNo"])

# Fill
## Fill all null values in columns of type String
df.na.fill("All Null values become this string")

## Fill all null values in columns of type String in specific columns
df.na.fill("all", subset=["StockCode", "InvoiceNo"])

# Replace
# The mos common use case is to replace all valies in a certain column according to their current value, but the only requirement is that this value has to be the same type as the original value
df.na.replace([""], ["UNKNOWN"], "Description")

# Ordering, to specify where you woul like your null values to appear
asc_nulls_first
desc_nulls_first
asc_nulls_last
desc_nulls_last
```

> Complex Types

```PYTHON
# Structs, are like dataframes within a dataframe
from pyspark.sql.functions import struct

complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")

## Query all columns
complexDF.select("complex.*")

# Arrays

## Split
from pyspark.sql.functions import split

df.select(split(col("Description"), " ")).show(2)

## Query array
df.select(split(col("Description"), " ").alias("array_col")).selectExpr("array_col[0]").show(2)

## Array Length
from pyspark.sql.functions import size

df.select(size(split(col("Description"), " "))).show(2) # shows 5 and 3

## Verify if there is a specific value in an array
from pyspark.sql.functions import array_contains

df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)

## Conver a complex type into a set of rows, similar to unnest 
from pyspark.sql.functions import split, explode

df.withColumn("splitted", split(col("Description"), " ")).withColumn("exploded", explode(col("splitted"))).select("Description", "InvoiceNo", "exploded").show(2)

# Maps
## Similar to dictionary is like an array with key-values
from pyspark.sql.functions import create_map

df.select(create_map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)

# Querying
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).selectExpr("complex_map['WHITE METAL LANTERN']").show(2)

# Explode maps
df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).selectExpr("explode(complex_map)").show(2)

+--------------------+------+
| key                | value|
+--------------------+------+
|WHITE HANGING HEA...|536365|
| WHITE METAL LANTERN|536365|
+--------------------+------+

```
## Filtering, dropping, sorting, and aggregating rows

- Filtering:

```PYTHON
# Filtering by passing a string
df.filter("columnName > number").show()

# Filter by passing a column of boolean values
df.filter(df.columnName > number).show()

# Filtering using the where() clause
df.where("condition").show()
```

- Dropping:
```PYTHON
# Removing columns
df.drop("COLUMN_NAME").columns
```

- Sorting:
```PYTHON
# API sort
df.sort(df.age.desc()).collect()

df.sort("age", ascending=False).collect()

df.orderBy(df.age.desc()).collect()

# Sort and orderBy
from pyspark.sql.functions import *

df.sort(asc("age")).collect()
df.orderBy(desc("age"), "name").collect()
df.orderBy(["age", "name"], ascending=[0, 1]).collect()
```

- Aggregating:
```PYTHON
# Count
from pyspark.sql.functions import count
df.select(count("StockCode")).show() # 541909

# countDistinct
from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() # 4070

# approx_count_distinct
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364

# first and last
from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()

# min and max
from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()

# sum
from pyspark.sql.functions import sum
df.select(sum("Quantity")).show() # 5176450

# sumDistinct
from pyspark.sql.functions import sumDistinct
df.select(sumDistinct("Quantity")).show() # 29310

# avg
from pyspark.sql.functions import sum, count, avg, expr
df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
.selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()
```

- Grouping
```PYTHON
# Just Grouping
df.groupBy("InvoiceNo", "CustomerId").count().show()

# Grouping with Expressions
from pyspark.sql.functions import count
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()
    
# Grouping with Maps
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)")).show()
```

- Window Functions **

You can also use window functions to carry out some unique aggregations by either computing some aggregation on a specific "window" of data, which you define by using a reference to the current data.

A window function calculates a return value for every input row of a table based on a group of rows, called a frame.

Kinds of window functions:

- Ranking functions
- Analytic functions
- Aggregate functions

```PYTHON
# 1. Create a column with only date format values
from pyspark.sql.functions import col, to_date

dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

# 2. Create a window specification
from pyspark.sql.window import Window
from pyspark.sql.functions import desc

windowSpec = Window\
.partitionBy("CustomerId", "date")\                         # Define how the group is going to be breaking
.orderBy(desc("Quantity"))\ 
.rowsBetween(Window.unboundedPreceding, Window.currentRow)  # States which rows will be included in the frame based on its reference to the current input row.

# 3. Apply and aggregation function
from pyspark.sql.functions import max

maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

# 4. Rank specific data
from pyspark.sql.functions import dense_rank, rank

purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

# 5. Select

from pyspark.sql.functions import col

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
.select(
col("CustomerId"),
col("date"),
col("Quantity"),
purchaseRank.alias("quantityRank"),
purchaseDenseRank.alias("quantityDenseRank"),
maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()

```

- Grouping Sets

Aggregations across multiple groups

> Rollups: 
 
Is a multidimensional aggregation that performs a variety of group-by style calculations
```PYTHON
rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
  .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
  .orderBy("Date")
rolledUpDF.show()

rolledUpDF.where("Country IS NULL").show()
rolledUpDF.where("Date IS NULL").show()
```

> Cube: 

Takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube does the same thing across all dimensions

```PYTHON
from pyspark.sql.functions import sum

dfNoNull.cube("Date", "Country").agg(sum(col("Quantity")))\
.select("Date", "Country", "sum(Quantity)").orderBy("Date").show()
```

> Pivot: 

Make it possible to convert a row into a column

```PYTHON
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

pivoted.where("date > '2011-12-05'").select("date" ,"`USA_sum(Quantity)`").show()
```

## joining, reading, writing and partitioning DataFrames
 
- Joining:

There are a variety of different join types available in Spark:

  - Inner joins (keep rows with keys that exist in the left and right datasets)
  - Outer joins (keep rows with keys in either the left or right datasets)
  - Left outer joins (keep rows with keys in the left dataset)
  - Right outer joins (keep rows with keys in the right dataset)
  - Left semi joins (keep the rows in the left, and only the left, dataset where the key appears in the right dataset)
  - Left anti joins (keep the rows in the left, and only the left, dataset where they do not appear in the right dataset)
  - Natural joins (perform a join by implicitly matching the columns between the two datasets with the same names)
  - Cross (or Cartesian) joins (match every row in the left dataset with every row in the right dataset) or inner joins that do not specify a predicate

```PYTHON
# Structure
df = df_1.join(df_2,on="key",how="typeOfJoinr"

# Inner Joins
# Form 1
joinExpression = person["graduate_program"] == graduateProgram['id']
wrongJoinExpression = person["name"] == graduateProgram["school"]
person.join(graduateProgram, joinExpression).show()
# Form 2
joinType = "inner"
person.join(graduateProgram, joinExpression, joinType).show()

# Outer Joins
joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()

# Left Outer Joins
joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show()

# Right Outer Joins
joinType = "right_outer"
person.join(graduateProgram, joinExpression, joinType).show(

# Left Semi Joins
joinType = "left_semi"
graduateProgram.join(person, joinExpression, joinType).show()

gradProgram2 = graduateProgram.union(spark.createDataFrame([
(0, "Masters", "Duplicated Row", "Duplicated School")]))
gradProgram2.createOrReplaceTempView("gradProgram2")
gradProgram2.join(person, joinExpression, joinType).show()

# Left Anti Joins
joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()
```

- Cross (Cartesian) Joins 

Will join every single row in the left DataFrame to every single row in the right DataFrame

```PYTHON
# Form 1
joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

# Form 2
script = "SELECT * FROM graduateProgram CROSS JOIN person " \
"ON graduateProgram.id = person.graduate_program"
person.crossJoin(graduateProgram).show()
```

- Reading and Writing from local sources

  - Reading:
 
    - Json Files
    ```PYTHON
    # Read json files
    # Implicit
    df = spark.read.format("json").option("path", json_file).load()
    # Explicit
    df = spark.read.json(json_file)
    ```
    - CSV Files
    ```PYTHON
    # Read csv files
    # Implicit
    df = (spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .option("mode", "FAILFAST")  # exit if any errors
        .option("nullValue", "")	  # replace any null data field with “”
        .option("path", csv_file)
        .load())
    # Explicit
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    ```
    - Parquet Files
    ```PYTHON
    # Read parquet files
    # Implicit
    df = (spark
        .read
        .format("parquet")
        .option("path", parquet_file)
        .load())
    # Explicit      
    df = spark.read.parquet(parquet_file)
    ```
  - Writing:
 
    - CSV Files
    ```PYTHON
    # Writing csv files
    # Implicit
    df.write.format('csv').save('filename.csv')
    # Explicit
    df.write.csv('filename.csv')
    ```
    - Json Files
    ```PYTHON
    # Writing json files
    # Implicit
    df.write.format('json').save('filename.json')
    # Explicit
    df.write.json('filename.json')
    ```
    - Parquet Files
    ```PYTHON
    # Writing parquet files
    # Implicit
    df.write.format('parquet').save('filename.parquet')
    # Explicit
    df.write.parquet('filename.parquet')
    ```
 
- Reading or Writing from external sources

  - Reading

    - Postgress
    ```PYTHON
    # Read Option 1: Loading data from a JDBC source using load method
    jdbcDF1 = (spark
    .read
    .format("jdbc")
    .option("url", "jdbc:postgresql://[DBSERVER]")
    .option("dbtable", "[SCHEMA].[TABLENAME]")
    .option("user", "[USERNAME]")
    .option("password", "[PASSWORD]")
    .load())
    # Read Option 2: Loading data from a JDBC source using jdbc method
    jdbcDF2 = (spark
    .read
    .jdbc("jdbc:postgresql://[DBSERVER]", "[SCHEMA].[TABLENAME]",
    properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))
    ```

    - Mongo
    ```PYTHON
    df = spark.read.format("mongo").option("uri",
    "mongodb://127.0.0.1/people.contacts").load()
    ```

  - Writing

    - Postgress
    ```PYTHON
    # Write Option 1: Saving data to a JDBC source using save method
    (jdbcDF1
    .write
    .format("jdbc")
    .option("url", "jdbc:postgresql://[DBSERVER]")
    .option("dbtable", "[SCHEMA].[TABLENAME]")
    .option("user", "[USERNAME]")
    .option("password", "[PASSWORD]")
    .save())
    # Write Option 2: Saving data to a JDBC source using jdbc method
    (jdbcDF2
    .write
    .jdbc("jdbc:postgresql:[DBSERVER]", "[SCHEMA].[TABLENAME]",
    properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))
    ```

    - Mongo
    ```PYTHON
    people.write.format("mongo").mode("append").option("database",
    "people").option("collection", "contacts").save()
    ```

- Partition:

  - A partition is a logical division of a large distributed data set
  - The number of partitions in an RDD can be found by using getNumPartitions() method

## Working with UDFs and Spark SQL functions
  
### UDFS 

- With SQL

```PYTHON
from pyspark.sql.types import LongType

# Create cubed function
def cubed(s):
return s * s * s

# Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test") 

# Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
```
 
- With DataFrame API

```PYTHON
import pandas as pd
# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Declare the cubed function 
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

# Create the pandas UDF for the cubed function 
cubed_udf = pandas_udf(cubed, returnType=LongType())

# Create a Spark DataFrame
df = spark.range(1, 4)

# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()
```

### Spark SQL
  
 ```PYTHON  
df = spark.read.parquet('yourFile.parquet')

df.createOrReplaceTempView('tableName')

df_2 = spark.sql('SELECT * FROM tableName WHERE condition')  
 ```  
  
## REFERENCES

- [PySpark Official Documentation](http://spark.apache.org/docs/latest/api/python/index.html)
- [Learning Spark](https://www.amazon.com/-/es/Jules-S-Damji/dp/1492050040/ref=sr_1_1__mk_es_US=%C3%85M%C3%85%C5%BD%C3%95%C3%91&dchild=1&keywords=Learning+Spark&qid=1609429204&sr=8-1)
- [Spark the Definitive Guide](https://www.amazon.com/-/es/Bill-Chambers/dp/1491912219/ref=sr_1_1?__mk_es_US=%C3%85M%C3%85%C5%BD%C3%95%C3%91&dchild=1&keywords=Spark+the+Definitive+Guide&qid=1609429241&sr=8-1)
- [DataCamp - Big Data Fundamentals with PySpark](https://www.datacamp.com/courses/big-data-fundamentals-with-pyspark)
