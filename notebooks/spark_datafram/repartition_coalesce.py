import spark
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("App1").getOrCreate()

data1=[(1, 'r'), (2,'s'), (3, 'd'),
       (4,'r'), (5,'s'), (6, 'd'),
       (1, 'r'), (2,'s'), (9, 'd'),
       (10, 'r'), (11,'s'), (12, 'd'),
       (13, 'r'), (14,'s'), (15, 'd')
       ]

schema=['id','charc']
df= spark.createDataFrame(data=data1,schema=schema)
df.show()

updated_df= df.repartition(4, "charc")
updated_df.show()