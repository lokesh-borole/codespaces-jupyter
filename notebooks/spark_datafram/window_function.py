import spark
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark=SparkSession.builder.appName('Window function').getOrCreate()
empData= [(1,'manish',50000,"IT","India"),
            (2,'vikash',60000,"sales","US"),
            (3,'raushan',70000,"marketing","US"),
            (4,'mukesh',50000,"IT","ENG"),
            (5,'pritam',90000,"sales","America"),
            (6,'nikita',45000,"marketing","India"),
            (7,'ragini',55000,"marketing","America"),
            (8,'rakesh',100000,"IT","India"),
            (9,'aditya',65000,"IT","China"),
            (10,'rahul',55000,"marketing","China")]

schema= ['id', 'Name', "Salary",'Dep',"Country"]
empDataframe= spark.createDataFrame(data=empData, schema=schema)
empDataframe.show()

#employees dataframe will be divided salary - partitonBy department
window=Window.partitionBy('dep')
empDataframe.withColumn('total_Salary', sum(col("salary")).over(window)).show()

## order by salary
window=Window.partitionBy('dep').orderBy('salary')
empDataframe.withColumn('total_Salary', sum(col("salary")).over(window)).show()

#dense_rank(), rank(), Row_number()

"""
  row_number()- assigns unique sequential integers to rows without handling ties.
  rank()-  assigns ranks to distinct values, leaving gaps in the ranking sequence for ties.
  dense_rank()- assigns ranks to distinct values, without leaving gaps for ties.

"""
empDataframe.withColumn('total_Salary', sum(col("salary")).over(window))\
    .withColumn('Rank', rank().over(window))\
    .withColumn('Row Number', row_number().over(window))\
    .withColumn('Dense Rank', dense_rank().over(window)).show()

