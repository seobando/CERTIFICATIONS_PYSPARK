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
 

> Joining:

```PYTHON
df = df_1.join(df_2,on="key",how="typeOfJoinr"
 ```
 
 > Reading and Writing from files:
 
 - Reading:
 
 ```PYTHON
# Read by changing the format

# Read json files


# Read csv files
people_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Read parquet files

 ```
 
 - Writing:
 
 ```PYTHON
# Writing by changing the format

# Writing csv files
spark.read.csv('path_file')

# Writing json files


# Writing parquet files
spark.read.parquet('path_file')
 ```
 
> Reading and Writing from files: 



> Partition:
 
 ```PYTHON
code
 ```
 
## working with UDFs and Spark SQL functions
  
  
## Other functions

- Collection functions
- Datetime functions
- Math functions
- Miscellaneous functions
- Non-aggregate functions

- Union and joins
- Windowing
- Modifications

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
