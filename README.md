# SPARK FUNDAMENTALS

This is a summary of my studies to the Spark Associate Developer for Apache Spark 3.0, the requirements [are](https://academy.databricks.com/exam/databricks-certified-associate-developer):

- have a basic understanding of the Spark architecture, including Adaptive Query Execution
- be able to apply the Spark DataFrame API to complete individual data manipulation task, including: 
  - selecting, renaming and manipulating columns
  - filtering, dropping, sorting, and aggregating rows
  - joining, reading, writing and partitioning DataFrames
  - working with UDFs and Spark SQL functions
 
## Spark arquitecture

## DataFrame API
 
 Page 145


Transformations

orderBy()
groupBy()
filter()
select()
join()

Actions

show()
take()
count()
collect()
save()
 
 
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
 
 > Reading and Writing from local sources
 
 - Reading:
 
 ```PYTHON
# Read json files
# Implicit
df = spark.read.format("json").option("path", json_file).load()
# Explicit
df = spark.read.json(json_file)

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
 
 ```PYTHON
# Writing csv files
# Implicit
df.write.format('csv').save('filename.csv')
# Explicit
df.write.csv('filename.csv')

# Writing json files
# Implicit
df.write.format('json').save('filename.json')
# Explicit
df.write.json('filename.json')

# Writing parquet files
# Implicit
df.write.format('parquet').save('filename.parquet')
# Explicit
df.write.parquet('filename.parquet')
 ```
 
> Reading or Writing from external sources

- Reading


- Writing


 
- Partition:
 
 ```PYTHON


 ```
 
## working with UDFs and Spark SQL functions
  
- UDFS with SQL

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
 
- UDFs with DataFrame API

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

- Spark SQL
  
 ```PYTHON  
df = spark.read.parquet('yourFile.parquet')

df.createOrReplaceTempView('tableName')

df_2 = spark.sql('SELECT * FROM tableName WHERE condition')  
 ```  
  
## Other functions

- Collection functions
- Datetime functions
- Math functions
- Miscellaneous functions
- Non-aggregate functions

- Union and joins
- Windowing
- Modifications


