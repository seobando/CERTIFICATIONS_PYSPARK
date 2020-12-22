# Apache Spark’s Structured APIs

## Spark: What’s Underneath an RDD?

The RDD is the most basic abstraction in Spark, with three vital characteristics:

- Dependencies: instructs Spark how an RDD is constructed with its inputs is required.
- Partitions (with some locality information): Spark the ability to split the work to parallelize computation on partitions across executors.
- Compute function: Partition => Iterator[T]: an RDD has a compute function that produces an Iterator[T] for the data that will be stored in the RDD

## Structuring Spark

Is about python, Python, R and SQL dealing with Spark.

An example with RDD API for Python:

```PYTHON
In Python
# Create an RDD of tuples (name, age)
dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30),
("TD", 35), ("Brooke", 25)])
# Use map and reduceByKey transformations with their lambda
# expressions to aggregate and then compute average
agesRDD = (dataRDD
.map(lambda x: (x[0], (x[1], 1)))
.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
.map(lambda x: (x[0], x[1][0]/x[1][1])))
```
The same exercise with a high-level DSL in python:

```PYTHON
# In Python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
# Create a DataFrame using SparkSession
spark = (SparkSession
.builder
.appName("AuthorsAges")
.getOrCreate())
# Create a DataFrame
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),("TD", 35), ("Brooke", 25)], ["name", "age"])
# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))
# Show the results of the final execution
avg_df.show()
```

## The DataFrame API

Spark DataFrames are like distributed in-memory tables with named columns and schemas, where each column has a specific data type: integer, string, array, map, real, date, timestamp, etc.

> Spark's Basic Data Types

ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BooleanType, DecimalType

> Spark’s Structured and Complex Data Types

BinaryType, TimestampType, DateType, ArrayType,MapType, StructType, StructField

> Schemas and Creating DataFrames

A schema in Spark defines the column names and associated data types for a Data‐Frame

> Two ways to define a schema

- Define it programmatically

```PYTHON
schema = StructType([StructField("author", StringType(), False),
  StructField("title", StringType(), False),
  StructField("pages", IntegerType(), False)])
```
- Employ a Data Definition Language (DDL)

```PYTHON
from pyspark.sql import SparkSession
# Define schema for our data using DDL
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING,
`Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

# Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
[2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter",
"LinkedIn"]],
[3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web",
"twitter", "FB", "LinkedIn"]],
[4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,
["twitter", "FB"]],
[5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web",
"twitter", "FB", "LinkedIn"]],
[6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,
["twitter", "LinkedIn"]]
]

# Main program
if __name__ == "__main__":
# Create a SparkSession
spark = (SparkSession
.builder
.appName("Example-3_6")
.getOrCreate())
# Create a DataFrame using the schema defined above
blogs_df = spark.createDataFrame(data, schema)
# Show the DataFrame; it should reflect our table above
blogs_df.show()
# Print the schema used by Spark to process the DataFrame
print(blogs_df.printSchema())
```

> Columns and Expressions

THERE IS NOT PYTHON EXAMPLE

> Rows

```PYTHON
# In Python
from pyspark.sql import Row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
["twitter", "LinkedIn"])
# access using index for individual items
blog_row[1]
'Reynold'
```

> Common DataFrame Operations

- DataFrameReader

```PYTHON
# In Python, define a schema
from pyspark.sql.types import *
# Programmatic way to define a schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
StructField('UnitID', StringType(), True),
StructField('IncidentNumber', IntegerType(), True),
StructField('CallType', StringType(), True),
StructField('CallDate', StringType(), True),
StructField('WatchDate', StringType(), True),
StructField('CallFinalDisposition', StringType(), True),
StructField('AvailableDtTm', StringType(), True),
StructField('Address', StringType(), True),
StructField('City', StringType(), True),
StructField('Zipcode', IntegerType(), True),
StructField('Battalion', StringType(), True),
StructField('StationArea', StringType(), True),
StructField('Box', StringType(), True),
StructField('OriginalPriority', StringType(), True),
StructField('Priority', StringType(), True),
StructField('FinalPriority', IntegerType(), True),
StructField('ALSUnit', BooleanType(), True),
StructField('CallTypeGroup', StringType(), True),
StructField('NumAlarms', IntegerType(), True),
StructField('UnitType', StringType(), True),
StructField('UnitSequenceInCallDispatch', IntegerType(), True),
StructField('FirePreventionDistrict', StringType(), True),
StructField('SupervisorDistrict', StringType(), True),
StructField('Neighborhood', StringType(), True),
StructField('Location', StringType(), True),
StructField('RowID', StringType(), True),
StructField('Delay', FloatType(), True)])
# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
```
- DataFrameWriter

```PYTHON
# In Python to save as a Parquet file
parquet_path = ...
fire_df.write.format("parquet").save(parquet_path)
```

Also:

```PYTHON
# In Python
parquet_table = ... # name of the table
fire_df.write.format("parquet").saveAsTable(parquet_table)
```

- Transformations and actions

SEE page 28

- Projections and filters

```PYTHON
# In Python
few_fire_df = (fire_df
.select("IncidentNumber", "AvailableDtTm", "CallType")
.where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)
```

- Renaming, adding, and dropping columns:

  - Renaming:

```PYTHON
# In Python
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
.select("ResponseDelayedinMins")
.where(col("ResponseDelayedinMins") > 5)
.show(5, False))  
```

  - Add and Drop
  
```PYTHON  
# In Python
fire_ts_df = (new_fire_df
.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
.drop("CallDate")
.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
.drop("WatchDate")
.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
"MM/dd/yyyy hh:mm:ss a"))
.drop("AvailableDtTm"))
```

- Aggregations
  
  - group(), count(), orderBy()

```PYTHON  
# In Python
(fire_ts_df
.select("CallType")
.where(col("CallType").isNotNull())
.groupBy("CallType")
.count()
.orderBy("count", ascending=False)
.show(n=10, truncate=False))
```

  - max(), min(), sum(), avg
  
```PYTHON  
#In Python
import pyspark.sql.functions as F
(fire_ts_df
.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
```

- Advaced statistical methos: stat(), describe(), correlation(),
covariance(), sampleBy(), approxQuantile(), frequentItems(),


## The Datset API

TOO TECHNICAL, dataframe is a row of a dataset or and alias for an untyped Dataset[Row], but there is not datasets in Python or R. There is more about this in Chapter 6.

## DataFrames Versus Datasets

- If you want to tell Spark what to do, not how to do it, use DataFrames or Datasets.
- If you want rich semantics, high-level abstractions, and DSL operators, use Data‐
Frames or Datasets.
- If you want strict compile-time type safety and don’t mind creating multiple case
classes for a specific Dataset[T], use Datasets.
- If your processing demands high-level expressions, filters, maps, aggregations,
computing averages or sums, SQL queries, columnar access, or use of relational
operators on semi-structured data, use DataFrames or Datasets.
- If your processing dictates relational transformations similar to SQL-like queries,
use DataFrames.
- If you want to take advantage of and benefit from Tungsten’s efficient serialization
with Encoders, , use Datasets.
- If you want unification, code optimization, and simplification of APIs across
Spark components, use DataFrames.
- If you are an R user, use DataFrames.
- If you are a Python user, use DataFrames and drop down to RDDs if you need
more control.
- If you want space and speed efficiency, use DataFrames.

There are some scenarios where you’ll want to consider using RDDs, such as when
you:
- Are using a third-party package that’s written using RDDs
- Can forgo the code optimization, efficient space utilization, and performance
benefits available with DataFrames and Datasets
- Want to precisely instruct Spark how to do a query

## Spark SQL and the Underlying Engine

- Unifies Spark components and permits abstraction to DataFrames/Datasets in
Java, Scala, Python, and R, which simplifies working with structured data sets.
- Connects to the Apache Hive metastore and tables.
- Reads and writes structured data with a specific schema from structured file formats
(JSON, CSV, Text, Avro, Parquet, ORC, etc.) and converts data into temporary
tables.
- Offers an interactive Spark SQL shell for quick data exploration.
- Provides a bridge to (and from) external tools via standard database JDBC/
ODBC connectors.
- Generates optimized query plans and compact code for the JVM, for final
execution.

>  The Catalyst Optimizer

1. Analysis
2. Logical optimization
3. Physical planning
4. Code generation

Catalog: a programmatic interfact to Spark SQL that holds a list of names of columns, data types, functions, tables, databases, etc.

IMPORTANT

End to End example page 68
