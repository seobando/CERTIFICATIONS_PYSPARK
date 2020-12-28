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
# Select the first set of columns
df.select("columnName_1","columnName_2","columnName_3").show()

# Select the second set of columns
df.select(df.columnName_1,df.columnName_2,df.columnName_3).show()

# Select with operation
df.select("columnName or Operation with column").alias("NewColumnName")).show()

# Select with expression
df.selectExpr("column as columnName to assign").show()
```

- Renaming:

```PYTHON
df = df.withColumnRenamed("Column", "NewColumnName")
```

- Manipulating columns:

```PYTHON
df.withColumn("NewColumn", df.columnName[some operation])
```

-- CONTROL FLOW WHEN / OTHERWISE

https://github.com/seobando/DATACAMP_DE/blob/main/Cleaning_Data_with_PySpark/2_Manipulating_DataFrames_in_the_real_world.md

## Filtering, dropping, sorting, and aggregating rows

- Filtering:

```PYTHON
# Filtering by passing a string
df.filter("columnName > number")

# Filter by passing a column of boolean values
df.filter(df.columnName > number)

# Filtering using the where() clause
df.select("column","column","column").where("condition")
```

## joining, reading, writing and partitioning DataFrames
 
- Joining:

```PYTHON
df = df_1.join(df_2,on="key",how="typeOfJoinr"
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
  
TEXT  
  
## Functions

- Collection functions
- Datetime functions
- Math functions
- Miscellaneous functions
- Non-aggregate functions
- Union and joins
- Windowing
- Modifications

## REFERENCES

- https://spark.apache.org/docs/latest/api/python/pyspark.html
- Learning Spark
