import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark= SparkSession.builder.appName('Yourapp').getOrCreate()

data1= [ (1, "Computer",332, 78),
          (2, 'ECE',222,98),
          (3, 'Mechanical',322, 45),
            (4, 'Civil',123, 89)

]
schema= ["id","Branch","Vaccency", "JEE_Score"]
cdf=spark.createDataFrame(data=data1, schema=schema)
cdf.show()

## avg(), sum(), count(), min(), max()
cdf.select(count('branch'))
cdf.select(count('branch').alias("Total branch")).show()

cdf.select(max('JEE_Score')).show()
cdf.select(min('JEE_Score')).show()
cdf.select(avg('JEE_Score')).show()
cdf.select(max('JEE_Score')).show()