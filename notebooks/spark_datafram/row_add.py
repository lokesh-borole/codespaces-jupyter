import spark
from pyspark.sql import SparkSession,Row 
from pyspark.sql.functions import *


## data frames are immutable we can't direclty modified it.
spark= SparkSession.builder.appName('Yourapp').getOrCreate()

data1= [ (1, "Computer",332),
          (2, 'ECE',222),
          (3, 'Mechanical',322)
]
schema= ["id","Branch","Vaccency"]
cdf=spark.createDataFrame(data=data1, schema=schema)
cdf.show()

##append a row into cdf 
new_row= Row(4, "Civil",422)

append_df= spark.createDataFrame(data=new_row, schema=schema)

append_df.union(cdf).show()
