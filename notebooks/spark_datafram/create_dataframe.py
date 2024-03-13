from pyspark.sql import SparkSession
import spark
from pyspark.sql.functions import *


spark=SparkSession.builder.appName('your app name').getOrCreate()
data1=[(1, 'r'),(2, 'j'),(3, 'k')]
my_schema=["id",'alphabets']


## create a dataframe api
create_df= spark.createDataFrame(data=data1,schema=my_schema)
#create_df.show()

## select a column from dataframe
#create_df.select('id').show()

## select multiple column from dataframe

#create_df.select('id','alphabets').show()

# select all columns from a dataframe 
#create_df.select("*")

# another method to select a column
# difference is that, we can perform transformation(opration by using 'col' function)
create_df.select(col('id')).show()

## each id will be increased by 2 
create_df.select(col("id")+2).show()

## expr method - for performing sql, mathematical and coumputing operations
create_df.select(expr("id+5")).show()