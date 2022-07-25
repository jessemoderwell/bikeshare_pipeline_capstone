from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
# import pyspark.pandas as pd
import geopy.distance
from shapely.geometry import Point
from shapely.geometry import Polygon as geopoly
from pyspark.sql.types import StructType, IntegerType, TimestampType, StringType, DoubleType
import configparser

#Initialize SparkSession
spark = SparkSession \
        .builder \
        .getOrCreate() \
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \

# Define bikeshare and e-scooter dataset schemas
bikeshare_schema = StructType() \
      .add("trip_id",IntegerType(),True) \
      .add("year",IntegerType(),True) \
      .add("month",IntegerType(),True) \
      .add("week",IntegerType(),True) \
      .add("day",IntegerType(),True) \
      .add("hour",IntegerType(),True) \
      .add("usertype",StringType(),True) \
      .add("gender",StringType(),True) \
      .add("starttime",TimestampType(),True) \
      .add("stoptime",TimestampType(),True) \
      .add("tripduration",DoubleType(),True) \
      .add("temperature",DoubleType(),True) \
      .add("events",StringType(),True) \
      .add("from_station_id",IntegerType(),True) \
      .add("from_station_name",StringType(),True) \
      .add("latitude_start",DoubleType(),True) \
      .add("longitude_start",DoubleType(),True) \
      .add("dpcapacity_start",DoubleType(),True) \
      .add("to_station_id",IntegerType(),True) \
      .add("to_station_name",StringType(),True) \
      .add("latitude_end",DoubleType(),True) \
      .add("longitude_end",DoubleType(),True) \
      .add("dpcapacity_end",DoubleType(),True) \

e_scooter_schema = StructType() \
      .add("Trip ID",StringType(),True) \
      .add("Start Time", StringType(),True) \
      .add("End Time", StringType(),True) \
      .add("Trip Distance",IntegerType(),True) \
      .add("Trip Duration",IntegerType(),True) \
      .add("Vendor",StringType(),True) \
      .add("Start Community Area Number",IntegerType(),True) \
      .add("End Community Area Number",IntegerType(),True) \
      .add("Start Community Area Name",StringType(),True) \
      .add("End Community Area Name",StringType(),True) \
      .add("Start Centroid Latitude",DoubleType(),True) \
      .add("Start Centroid Longitude",DoubleType(),True) \
      .add("Start Centroid Location",StringType(),True) \
      .add("End Centroid Latitude",DoubleType(),True) \
      .add("End Centroid Longitude",DoubleType(),True) \
      .add("End Centroid Location",StringType(),True) 

# Ingest files from S3
e_scooter_data = ('')
bikeshare_data = ('')
comm_data = ('')

bikeshare_df = spark.read.format("csv") \
      .option("header", True) \
      .schema(bikeshare_schema) \
      .load(bikeshare_data)

e_scooter_df = spark.read.format("csv") \
.option("header",True) \
.schema(e_scooter_schema) \
.load(e_scooter_data)

comm_df = spark.read.option("header",True).csv(comm_data)

# Index and filter bikeshare and e-scooter dataframes
bikeshare_df = bikeshare_df.select('trip_id', 'gender', 'usertype', 'starttime', \
                    'stoptime', 'tripduration', 'latitude_start', \
                    'longitude_start', 'latitude_end', 'longitude_end')

e_scooter_df = e_scooter_df.select(f.col('TRIP ID').alias('trip_id'), f.col('Start Time').alias('starttime'), \
                                   f.col('End Time').alias('endtime'), f.col('Trip Distance').alias('trip_distance'), \
                                   f.col('Trip Duration').alias('trip_duration'), f.col('Vendor').alias('vendor'), \
                                   f.col('Start Community Area Name').alias('start_neighborhood'), \
                                   f.col('End Community Area Name').alias('end_neighborhood')) 

# Convert seconds to minutes in e-scooter DataFrame
to_minutes = f.udf(lambda x: round((int(x)/60), 2))
e_scooter_df = e_scooter_df.withColumn('trip_duration', to_minutes('trip_duration'))

# Calculate Distance for Bikeshare DataFrame
@f.udf
def calculate_dist(x1, y1, x2, y2):
    coords_1 = (x1, y1)
    coords_2 = (x2, y2)
    dist = geopy.distance.geodesic(coords_1, coords_2).m
    return round(dist, 2)

bikeshare_df = bikeshare_df.withColumn('trip_distance', calculate_dist('latitude_start', 'longitude_start', \
                                                                  'latitude_end', 'longitude_end'))
# Round bikeshare df trip duration to just 2 digits
round_dur = f.udf(lambda x: round(x, 2))

bikeshare_df = bikeshare_df.withColumn('tripduration', round_dur(bikeshare_df.tripduration))

# Generate start/ end neighborhood for Bikeshare dataframe

# Start by converting community and bikeshare dataframes to pandas
comm_df = comm_df.toPandas()
bikeshare_df = bikeshare_df.toPandas()

# Create list of neighborhood polygons
def gen_polygons(g):
    poly_list = []
    for m in g:
        m = m[16:-3].split(', ')
        m = [x.replace('(','').replace(')','') for x in m]
        coord_tuple_list = []
        for coords in m:
            coord_list = coords.split(' ')
            coord_tuple = (float(coord_list[1]), float(coord_list[0]))
            coord_tuple_list.append(coord_tuple)
        poly_list.append(geopoly(coord_tuple_list))
    return poly_list

poly_list = gen_polygons(comm_df.the_geom)

# Function to check lat/ long points against neighborhood polygons
def get_neighborhood(lat, long, poly_list):
    neighborhoods = []
    for i in range(len(lat)):
        for j, poly in enumerate(poly_list):
            P = Point(lat[i], long[i])
            if poly.contains(P):
                neighborhoods.append(j)
                break
            elif j >= len(poly_list) - 1:
                neighborhoods.append("N/A")
                break
    return neighborhoods

start_neighborhood = get_neighborhood(bikeshare_df.latitude_start, bikeshare_df.longitude_start, poly_list)
end_neighborhood = get_neighborhood(bikeshare_df.latitude_end, bikeshare_df.longitude_end, poly_list)

# Map list of community indexes to get series of neighborhoods
def id_to_neighborhood(comm_n, community):
    n = []
    for x in comm_n:
        if x == "N/A":
            n.append("N/A")
        else:
            n.append(community[x])
    return n

bikeshare_df['start_neighborhood'] = id_to_neighborhood(start_neighborhood, comm_df.COMMUNITY)
bikeshare_df['end_neighborhood'] = id_to_neighborhood(end_neighborhood, comm_df.COMMUNITY)

# Convert bikeshare dataframe back to pyspark dataframe
bikeshare_df = spark.createDataFrame(bikeshare_df)

# Convert trip distance and duration to datatype double
bikeshare_df = bikeshare_df.withColumn('trip_distance', bikeshare_df.trip_distance.cast(DoubleType()))
e_scooter_df = e_scooter_df.withColumn('trip_duration', e_scooter_df.trip_duration.cast(DoubleType()))

# Convert starttime and endtime in e-scooter dataframe to timestamps
to_tmstmp = f.udf(lambda x:datetime.strptime(x, '%m/%d/%Y %H:%M:%S %p'))
e_scooter_df = e_scooter_df.withColumn('starttime', to_tmstmp('starttime'))
e_scooter_df = e_scooter_df.withColumn('endtime', to_tmstmp('endtime'))
to_timestr = f.udf(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
e_scooter_df = e_scooter_df.withColumn('starttime', to_timestr('starttime'))
e_scooter_df = e_scooter_df.withColumn('endtime', to_timestr('endtime'))
e_scooter_df = e_scooter_df.withColumn('starttime', e_scooter_df.starttime.cast(TimestampType()))
e_scooter_df = e_scooter_df.withColumn('endtime', e_scooter_df.endtime.cast(TimestampType()))

# Write bikeshare and e-scooter pyspark dataframes to s3 (Parquet)
s3_output = 's3a://airflow-bikeshare/'

bikeshare_df.write.mode('overwrite').parquet(s3_output + 'bikeshare_rides.parquet')
e_scooter_df.write.mode('overwrite').parquet(s3_output + 'e_scooter_rides.parquet')