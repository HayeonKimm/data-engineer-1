from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession \
    .builder \
    .appName("weather") \
    .getOrCreate()

weatherSchema = spark.read.format('json').load('/home/lab01/weather/20210810095902Weatherx59y124.json').schema
weatherDf = spark.readStream.schema(weatherSchema).json('/home/lab01/weather')
df_weather = weatherDf.select(explode(weatherDf.response.body.items.item).alias("weather")).select('weather.*')
df_weather.coalesce(1).writeStream.format('json').option("checkpointLocation", "/home/lab01/weather_after_check").option("path", "/home/lab01/weather_after").trigger(processingTime='21600 seconds').start().awaitTermination()

