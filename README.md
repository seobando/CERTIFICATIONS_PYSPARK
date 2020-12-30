# SPARK FUNDAMENTALS

This is a summary of my studies to the Spark Associate Developer for Apache Spark 3.0, the requirements [are](https://academy.databricks.com/exam/databricks-certified-associate-developer):

- have a basic understanding of the Spark architecture, including Adaptive Query Execution
- be able to apply the Spark DataFrame API to complete individual data manipulation task, including: 
  - selecting, renaming and manipulating columns
  - filtering, dropping, sorting, and aggregating rows
  - joining, reading, writing and partitioning DataFrames
  - working with UDFs and Spark SQL functions
 
## Spark arquitecture

TEXT

## DataFrame API
 
Spark is a distributed programming model in which the user specifies transformations. Multiple transformations build up a directed acyclic graph of instructions. An action begins the process of executing that graph of instructions, as a single job, by breaking it down into stages and tasks to execute across the cluster. The logical structures that we manipulate with transformations and actions are DataFrames and Datasets. To create a new DataFrame or Dataset, you call a transformation. To start computation or convert to native language types, you call an action.

Transformations examples: orderBy(), groupBy(), filter(), select(), join()
Actions examples: show(), take(), count(), collect(), save()
 
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

- Working with Strings
```PYTHON
#
from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()

#
from pyspark.sql.functions import lower, upper
df.select(col("Description"),
lower(col("Description")),
upper(lower(col("Description")))).show(2)

#
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
ltrim(lit(" HELLO ")).alias("ltrim"),
rtrim(lit(" HELLO ")).alias("rtrim"),
trim(lit(" HELLO ")).alias("trim"),
lpad(lit("HELLO"), 3, " ").alias("lp"),
rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)

# Regular Expressions

##
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
col("Description")).show(2)

##
from pyspark.sql.functions import translate
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
.show(2)

##
from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
col("Description")).show(2)

##
from pyspark.sql.functions import instr
containsBlack = instr(col("Description"), "BLACK") >= 1
containsWhite = instr(col("Description"), "WHITE") >= 1
df.withColumn("hasSimpleColor", containsBlack | containsWhite)\
.where("hasSimpleColor")\
.select("Description").show(3, False)

##
```

- Working with Dates and Timestamps

```PYTHON
#
from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)

#
dateDF.withColumn("week_ago", date_sub(col("today"), 7)).select(datediff(col("week_ago"), col("today"))).show(1)
dateDF.select(to_date(lit("2016-01-01")).alias("start"),to_date(lit("2017-05-22")).alias("end")).select(months_between(col("start"), col("end"))).show(1)

#
from pyspark.sql.functions import to_date, lit
spark.range(5).withColumn("date", lit("2017-01-01")).select(to_date(col("date"))).show(1)

#
from pyspark.sql.functions import to_date

dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")

#
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
```

- Working with Nulls in Data

```PYTHON
# Coalesce
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()
```

- Drop

```PYTHON
#
df.na.drop()
df.na.drop("any")

#
df.na.drop("all")

#
df.na.drop("all", subset=["StockCode", "InvoiceNo"])
```

- Fill
```PYTHON
#
df.na.fill("All Null values become this string")

#
df.na.fill("all", subset=["StockCode", "InvoiceNo"])
```

- Replace
```PYTHON
df.na.replace([""], ["UNKNOWN"], "Description")
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

TEXT

- Rollups **

TEXT

- Cube **

TEXT

- Pivto **



- Sorting:
```PYTHON
df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
```

## joining, reading, writing and partitioning DataFrames
 
- Joining:

```PYTHON
# Structure
df = df_1.join(df_2,on="key",how="typeOfJoinr"

# Inner Joins
#
joinExpression = person["graduate_program"] == graduateProgram['id']
wrongJoinExpression = person["name"] == graduateProgram["school"]
person.join(graduateProgram, joinExpression).show()
#
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

- Cross (Cartesian) Joins **

```PYTHON
#
joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

#
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

```PYTHON
PENDING
```
 
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

- https://spark.apache.org/docs/latest/api/python/pyspark.html
- Learning Spark
- Spark the Definitive Guide
- DataCamp - Big Data Fundamentals with PySpark
