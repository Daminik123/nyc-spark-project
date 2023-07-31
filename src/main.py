from pyspark.sql import SparkSession, Window
from pyspark import SparkConf
import pyspark.sql.types as t
import pyspark.sql.functions as f

def get_spark_session():
    spark_session = (SparkSession.builder
                                     .master("local")
                                     .appName("my_test_app")
                                     .config(conf=SparkConf())
                                     .getOrCreate())
    return spark_session

def get_df_schema():
    df_schema = t.StructType([
        t.StructField("medallion", t.StringType(), True),
        t.StructField("hack_license", t.StringType(), True),
        t.StructField("vendor_id", t.StringType(), True),
        t.StructField("rate_code", t.StringType(), True),
        t.StructField("store_and_fwd_flag", t.StringType(), True),
        t.StructField("pickup_datetime", t.StringType(), True),
        t.StructField("dropoff_datetime", t.StringType(), True),
        t.StructField("passenger_count", t.IntegerType(), True),
        t.StructField("trip_time_in_secs", t.IntegerType(), True),
        t.StructField("trip_distance", t.DecimalType(5,2), True),
        t.StructField("pickup_longitude", t.DecimalType(9,6), True),
        t.StructField("pickup_latitude", t.DecimalType(9,6), True),
        t.StructField("dropoff_longitude", t.DecimalType(9,6), True),
        t.StructField("dropoff_latitude", t.DecimalType(9,6), True)
    ])
    return df_schema

csv_files = [
    "dataset/trip_data_1.csv",
    "dataset/trip_data_2.csv",
    "dataset/trip_data_3.csv",
    "dataset/trip_data_4.csv",
    "dataset/trip_data_5.csv",
    "dataset/trip_data_6.csv",
    "dataset/trip_data_7.csv",
    "dataset/trip_data_8.csv",
    "dataset/trip_data_9.csv",
    "dataset/trip_data_10.csv",
    "dataset/trip_data_11.csv",
    "dataset/trip_data_12.csv"
]

spark = get_spark_session()
df_schema = get_df_schema()

df = None
for file_path in csv_files:
    df = spark.read.csv(file_path,
                    header=True,
                    nullValue='',
                    schema=df_schema)
    if df is None:
        df = df
    else:
        df = df.union(df)

print(df.count())
df.show()
df.printSchema()
df.describe().show()

df = df.filter((f.col('passenger_count') < 10) & (f.col('passenger_count') != 0))

#1
highest_avg_trip_distance_by_vendor_id = df.groupBy('vendor_id')\
                                           .agg(f.avg('trip_distance')\
                                           .alias('avg_trip_distance'))\
                                           .orderBy(f.desc(f.col('avg_trip_distance')))
highest_avg_trip_distance_by_vendor_id.show()

del highest_avg_trip_distance_by_vendor_id

#2
avg_trip_distance_and_time_for_diff_num_of_passengers = df.groupBy('passenger_count')\
                                                          .agg(f.avg('trip_distance').alias('avg_trip_distance'),
                                                               f.avg('trip_time_in_secs').alias('avg_trip_time_in_secs'))\
                                                          .orderBy(f.desc(f.col('avg_trip_distance')))
avg_trip_distance_and_time_for_diff_num_of_passengers.show()
df.join(avg_trip_distance_and_time_for_diff_num_of_passengers, on='passenger_count', how='left').show()

del avg_trip_distance_and_time_for_diff_num_of_passengers

#3
df_2 = df.withColumn('pickup_datetime', f.col('pickup_datetime').cast('timestamp'))
peak_hours = df_2.withColumn('pickup_hour', f.hour(f.col('pickup_datetime')))
peak_hours = peak_hours.groupBy("pickup_hour").count().orderBy(f.col("count").desc())
peak_hours.show(peak_hours.count(), truncate=False)

del peak_hours
del df_2

#4
df = df.filter(~((f.col('pickup_longitude') == 0) 
                    & (f.col('pickup_latitude') == 0) 
                    & (f.col('dropoff_longitude') == 0) 
                    & (f.col('dropoff_latitude') == 0)))

highest_taxi_demand_areas = df.groupBy('pickup_longitude', 'pickup_latitude')\
                              .agg(f.count('*')
                              .alias('count'))\
                              .orderBy(f.desc('count'))
highest_taxi_demand_areas.show()

del highest_taxi_demand_areas

#5
window = Window.partitionBy('medallion')
total_trip_time_and_distance_per_medallion = df.withColumn('total_trip_distance', f.sum('trip_distance')
                                               .over(window))\
                                               .withColumn('total_trip_time', f.sum('trip_time_in_secs')
                                               .over(window))\
                                               .sort(f.desc('total_trip_distance'))
total_trip_time_and_distance_per_medallion.show(100, truncate=False)

del total_trip_time_and_distance_per_medallion

#6
window = Window.partitionBy('hack_license')
max_passenger_count_per_trip = df.withColumn('max_passenger_count', f.max('passenger_count')
                                 .over(window))\
                                 .sort(f.desc('max_passenger_count'))
max_passenger_count_per_trip.show(100, truncate=False)

del max_passenger_count_per_trip

#7
df = df.withColumn('pickup_datetime', f.col('pickup_datetime')
       .cast('timestamp'))\
       .withColumn('dropoff_datetime', f.col('dropoff_datetime')
       .cast('timestamp'))
driver_id, time_from, time_to = '9FD8F69F0804BDB5549F40E9DA1BE472', 18, 19
df.filter((f.col('hack_license') == driver_id)
           & (f.hour(f.col('pickup_datetime')) >= time_from) 
           & (f.hour(f.col('pickup_datetime')) < time_to))\
  .orderBy('pickup_datetime').show()