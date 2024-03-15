import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark= SparkSession.builder.appName('Group By').getOrCreate()

empData= [(1,'manish',50000,"IT","India"),
            (2,'vikash',60000,"sales","US"),
            (3,'raushan',70000,"marketing","US"),
            (4,'mukesh',80000,"IT","ENG"),
            (5,'pritam',90000,"sales","America"),
            (6,'nikita',45000,"marketing","India"),
            (7,'ragini',55000,"marketing","America"),
            (8,'rakesh',100000,"IT","India"),
            (9,'aditya',65000,"IT","China"),
            (10,'rahul',50000,"marketing","China")]

schema= ['id', 'Name', "Salary",'Dep',"Country"]
empDataframe= spark.createDataFrame(data=empData, schema=schema)
empDataframe.show()


## group by department

empDataframe.groupBy('dep').agg(sum("salary")).show()
empDataframe.groupBy('dep',"country").agg(sum("salary")).show()


agg_df = empDataframe.groupBy('dep', 'country').agg(sum("salary").alias("total_salary"))

# select name, id, 
# Then, join the aggregated DataFrame with the original DataFrame to get the names
result_df = agg_df.join(empDataframe, on=['dep', 'country']).select('name', 'dep', 'country', 'total_salary')

# Show the result
result_df.show()

#how we can perform in Spark Sql

empDataframe.createOrReplaceTempView('Employees')

print("-----------Spark Sql------------- ")

spark.sql(""" 
     select * from Employees    
""").show()


print("Group by Dep")
spark.sql(""" 
     SELECT dep, SUM(salary) AS total_salary
     FROM Employees
     GROUP BY dep
""").show()