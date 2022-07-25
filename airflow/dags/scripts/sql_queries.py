import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

staging_bikeshare_create = ("""
create table if not exists staging_bikeshare_rides
(trip_id bigint CONSTRAINT staging_bikehshare_rides_pk PRIMARY KEY, 
gender varchar(250), usertype varchar(250),
starttime timestamp, stoptime timestamp, tripduration varchar(250),
latitude_start double precision, longitude_start double precision, 
latitude_end double precision, longitude_end double precision, trip_distance double precision,
start_neighborhood varchar(250), end_neighborhood varchar(250))
"""
)

staging_e_scooter_create = ("""
create table if not exists staging_e_scooter_rides
(trip_id varchar CONSTRAINT staging_e_scooter_rides_pk PRIMARY KEY, 
starttime timestamp, endtime timestamp, 
trip_distance int, trip_duration double precision, vendor varchar,
start_neighborhood varchar, end_neighborhood varchar)
"""
)

staging_bikeshare_copy = ("""copy staging_bikeshare_rides
from '{}'
iam_role '{}'
FORMAT AS PARQUET
compupdate off 
""").format(config['S3']['BIKESHARE_DATA'], config['IAM_ROLE']['ARN'])

staging_e_scooter_copy = ("""copy staging_e_scooter_rides
from 's3://airflow-bikeshare/e_scooter_rides.parquet'
iam_role '{}'
FORMAT AS PARQUET
compupdate off 
""").format(config['S3']['E_SCOOTER_DATA'], config['IAM_ROLE']['ARN'])

neighborhood_create = ("""
create table if not exists neighborhood_metrics
(avg_trip_dist double precision, avg_trip_dur double precision, 
end_neighborhood varchar, vehicle varchar, 
PRIMARY KEY (end_neighborhood, vehicle))
""")

neighborhood_metrics = ("""
    insert into neighborhood_metrics
    select * from
(select round(AVG(s.trip_distance), 2) as avg_trip_dist, round(AVG(trip_duration), 2) as avg_trip_dur, s.end_neighborhood as end_neighborhood, 'scooter' as vehicle
from staging_e_scooter_rides s
where s.end_neighborhood in (select distinct end_neighborhood from staging_bikeshare_rides)
group by s.end_neighborhood
union
select round(AVG(b.trip_distance), 2) as avg_trip_dist, round(AVG(b.tripduration), 2) as avg_trip_dur, b.end_neighborhood as end_neighborhood, 'bike' as vehicle
from staging_bikeshare_rides b
group by b.end_neighborhood) sub
order by end_neighborhood
"""
)

copy_table_queries = [staging_bikeshare_copy, staging_e_scooter_copy, neighborhood_metrics]
create_table_queries = [staging_bikeshare_create, staging_e_scooter_create, neighborhood_create]

