import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark= SparkSession.builder.appName('Yourapp').getOrCreate()

data1= [ (1, "Computer",332),
          (2, 'ECE',222),
          (3, 'Mechanical',322)
]
schema= ["id","Branch","Vaccency"]
cdf=spark.createDataFrame(data=data1, schema=schema)
cdf.show()

data2= [ (4, "Chemical",332),
          (5, 'Electrical',222),
          (6, 'Civil',322)
]
schema= ["id","Branchj","Vaccency"]
cdf2=spark.createDataFrame(data=data2, schema=schema)
cdf2.show()

## union data 
cdf.union(cdf2).show()
cdf.unionAll(cdf2).show()
c=cdf.unionAll(cdf2).count() # total number of rows
print(c)
#cdf.unionByName(cdf2).show()  ## here will got an error, because of 'branch' and 'branchj'  