import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("app").getOrCreate()

csv_data= """1,albert_park,Albert Park Grand Prix Circuit,Melbourne,Australia,-37.8497,144.968,10,http://en.wikipedia.org/wiki/Melbourne_Grand_Prix_Circuit
2,sepang,Sepang International Circuit,Kuala Lumpur,Malaysia,2.76083,101.738,,http://en.wikipedia.org/wiki/Sepang_International_Circuit
3,bahrain,Bahrain International Circuit,Sakhir,Bahrain,26.0325,50.5106,,http://en.wikipedia.org/wiki/Bahrain_International_Circuit
4,catalunya,Circuit de Barcelona-Catalunya,Montmel�_,Spain,41.57,2.26111,,http://en.wikipedia.org/wiki/Circuit_de_Barcelona-Catalunya
5,istanbul,Istanbul Park,Istanbul,Turkey,40.9517,29.405,,http://en.wikipedia.org/wiki/Istanbul_Park
6,monaco,Circuit de Monaco,Monte-Carlo,Monaco,43.7347,7.42056,,http://en.wikipedia.org/wiki/Circuit_de_Monaco
7,villeneuve,Circuit Gilles Villeneuve,Montreal,Canada,45.5,-73.5228,,http://en.wikipedia.org/wiki/Circuit_Gilles_Villeneuve
8,magny_cours,Circuit de Nevers Magny-Cours,Magny Cours,France,46.8642,3.16361,,http://en.wikipedia.org/wiki/Circuit_de_Nevers_Magny-Cours
9,silverstone,Silverstone Circuit,Silverstone,UK,52.0786,-1.01694,,http://en.wikipedia.org/wiki/Silverstone_Circuit
10,hockenheimring,Hockenheimring,Hockenheim,Germany,49.3278,8.56583,,http://en.wikipedia.org/wiki/Hockenheimring
11,hungaroring,Hungaroring,Budapest,Hungary,47.5789,19.2486,,http://en.wikipedia.org/wiki/Hungaroring
12,valencia,Valencia Street Circuit,Valencia,Spain,39.4589,-0.331667,,http://en.wikipedia.org/wiki/Valencia_Street_Circuit
13,spa,Circuit de Spa-Francorchamps,Spa,Belgium,50.4372,5.97139,,http://en.wikipedia.org/wiki/Circuit_de_Spa-Francorchamps
14,monza,Autodromo Nazionale di Monza,Monza,Italy,45.6156,9.28111,,http://en.wikipedia.org/wiki/Autodromo_Nazionale_Monza
15,marina_bay,Marina Bay Street Circuit,Marina Bay,Singapore,1.2914,103.864,,http://en.wikipedia.org/wiki/Marina_Bay_Street_Circuit
16,fuji,Fuji Speedway,Oyama,Japan,35.3717,138.927,,http://en.wikipedia.org/wiki/Fuji_Speedway"""

csv_split= csv_data.strip().split('\n')

clean_data= [tuple(i.strip().split(",")) for i in csv_split]

schema=["circuitId","circuitRef","name","location","country","lat","lng","alt","url"]
df= spark.createDataFrame(data=clean_data, schema=schema)
df.show()
#df.printSchema()


#df.distinct().show()

## disctinct column values

#df.select('circuitRef', "location").distinct().show()

## drop duplicates data 

duplicateNo= df.drop_duplicates(["circuitId","circuitRef","name","location","country","lat","lng","alt","url"])
#duplicateNo.show()


## sort the data 
duplicateNo.sort(col("lat").desc()).show()
