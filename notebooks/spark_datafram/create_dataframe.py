from pyspark.sql import SparkSession
import spark
from pyspark.sql.functions import *


spark=SparkSession.builder.appName('your app name').getOrCreate()
data1=[(1, 'r', 34),(2, 'j', 45),(3, 'k', 67)]
my_schema=["id",'alphabets', 'num']


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
#create_df.select(col('id')).show()

## each id will be increased by 2 
#create_df.select(col("id")+2).show()

## expr method - for performing sql, mathematical and coumputing operations
#create_df.select(expr("id+5")).show()

## alias a column name

create_df.select(col('id').alias("alpha id")).show()

# filter  and where
create_df.filter(col("id")==1).show()
# both are similar
create_df.where(col("id")==1).show()

create_df.where(col('num')>35).show()

create_df.filter((col('num')>35) & (col('id')==2)).show()

# literal
# literal= common value in each record for a column
create_df.select("*",lit('lowercase').alias('case')).show()
create_df.select("*", lit('none').alias("feature")).show()

# withColumn, create a new column without select method

create_df.withColumn('maxvalue', lit(2)).show()

# renamecolumn name
create_df.withColumnRenamed("id", "cid").show()

#casting (conversion of data types)

create_df.withColumn('num', col('num').cast('long')).printSchema() #
create_df.select(col('num').cast('long')).printSchema() # for selected column

#delete a column
create_df.drop(col('num')).show()

create_df.show()

