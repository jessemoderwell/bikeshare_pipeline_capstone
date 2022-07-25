# Airflow Pipeline to Data Warehouse: Bikeshare and E-scooter rides

Project scope: For this project, I will be accessing several datasets involving bike and e-scooter sharing in the city of Chicago. 'e-sccoter_ride_stats.csv'  contains records for each trip residents took on a scooter during the 2020 pilot program. It includes columns for trip distance, duration, start and end time, and start and end neighborhood, among other information. 'divvy_ride_stats.csv' provides data from bike share company Divvy on customer rides similar to the scooters. Similar fields are available as the scooters, some of them being generated in the data pipeline. I'll also be using a dataset with Chicago neighborhood boundaries to add the start and end neighborhood feature to bike share data. I'd like the end use case for this project to be an analytics table that can provide insights on bike and scooter rides in each Chicago neighborhood. I'd also like them to be hooked up to dashboards or BI applications for visualization.

Scripts and dag to transform raw ride data into database formatted for analytics using Python, Airflow, AWS EMR, AWS Redshift, PostreSQL, and Pyspark

## Table of contents

- General info
- Data Dictionary
- Files Summary
- Technologies
- Additional Considerations
- Setup
- Inspiration

### Data Dictionary

BIKESHARE RIDES

Field | Data type | Description

<ol>
<li>trip_id | bigint | a unique id that identifies each ride </li>

<li>gender | varchar(250) | gender of the rider, either M or F</li>

<li>usertype | varchar(250) | "Customer" is a rider who purchased a 24-Hour Pass; "Subscriber" is a rider who purchased an Annual Membership</li>

<li>starttime | timestamp | day and time trip started, in CST</li>

<li>stoptime | timestamp| day and time trip ended, in CST</li>

<li>tripduration | varchar(250) | time of trip in minutes</li>

<li>latitude_start | double precision | start station latitude</li>

<li>longitude_start | double precision | start station longitude</li>

<li>latitude_end | double precision | end station latitude</li>

<li>longitude_end | double precision | end station longitude</li>

<li>trip_distance | double precision | distance from start station to end station, in meters</li>

<li>start_neighborhood | varchar(250) | chicago neighborhood start station is contained in</li>

<li>end_neighborhood | varchar(250) | chicago neighborhood end station is contained in</li>

</ol>

This dataset comes from Kaggle: https://www.kaggle.com/datasets/yingwurenjian/chicago-divvy-bicycle-sharing-data

E-SCOOTER RIDES

Field | Data type | Description

<ol>

<li>trip_id | varchar | unique id for each scooter ride</li>

<li>starttime | timestamp | day and time trip started, in CST</li>

<li>endtime | timestamp | day and time trip ended, in CST</li>

<li>trip_distance | int | distance from start point to end, in meters</li>

<li>trip_duration | double precision | time of trip in minutes</li>

<li>vendor | varchar | vendor of the scooter</li>

<li>start_neighborhood | varchar | neighborhood where scooter trip started</li>

<li>end_neighborhood | varchar | neighborhood where scooter trip ended</li>

</ol>

This dataset is courtesy of the Chicago Data Portal, and came from the 2020 Chicago pilot program: https://data.cityofchicago.org/Transportation/E-Scooter-Trips-2020/3rse-fbp6/data

### General Info
This database allows dashboards and BI applications to easily reference, and potentially visualize in a geographical format. The design of the schema and tables is e-scooter and bikeshare rides in two separate staging tables, then formatted in an analytics table broken down by neighborhood. Each staging table has unique Id's for each ride (primary keys)

### Files Summary
The data sources for the staging tables come from one s3 bucket: Log data comes in the form of csv's from 's3://bikeshare-project'. 'emr_script.py' is stored in s3 under 's3://bikeshare-script', and is added as a step for the emr cluster via the add steps task in airflow. After transforming and cleaning the csv files, emr_script writes two parquet files to 's3://airflow-bikeshare'. etl.py, sql_queries.py, and dwh.cfg are all stored in the scripts directory under dags (local), and they are responsible for creating and loading the redshift tables

dwh.config holds the usernames, passwords, ARN resource, and aws redshift database endpoint for connection. sql_queries.py contains all the sql queries as strings for the script to run

### Technologies
Python 3.6.3
configparser 5.2.0
psycopg2 2.7.4
geopy 2.2.0
pandas 1.3.5
Shapely 1.8.2
PySpark 3.2.0

The technology we are using for our data warehouse, which is where our transformed analytical data resides, is AWS Redshift. Redshift uses PostgreSQL as it's query language, and incoming log and song data are housed in S3

### Additional Considerations
These are some changes I'd make to my project were the following scenarios to take place:

Q1: What if the data was increased by 100x?

A1: If the data were increased by 100x, I would need to change the part of the emr_script where it uses pandas and an iterative approach to generating the start and end neighborhood for the bikeshare dataframe. I originally tried to use a udf and functional programming approach to do this, but it seemed the pyspark dataframe was unable to handle a polygon object as an input, so I'd be back to the drawing board for that. Were I able to do that, I'd also increase the amount of slave nodes I have in EMR, to improve the performance.

On the redshift side, I would likely implement a distribution method to make the neighborhood metrics query more efficient. I would try a sort key on distance and duration in the bike/ scooter tables, even though my query is aggregrating those columns to get the average, so there may be some shuffling

Q2: What if the pipeline would be run on a daily basis by 7 am every day?

A2: I think I might be pretty well prepared for this scenario, though I might want to consider changing the redshift creation to use boto3 programatically instead of using the console. This wouldn't actually make the whole job programmatic, since the connection in airflow needs to be created and filled out, but creating the cluster programmatically may save some time, so that's one consideration since people are not up to create it. Although in that case, it might actually be easier to just have the redshift cluster spun up all the time, and then have the dag schedule be set to daily

Q3: What if he database needed to be accessed by 100+ people?

A3: If that were the case, I would use the create user and alter group sql statements to make the database accessible to the group. They would then just need the endpoint, which I could supply them, and the port (5439). They could then use boto3 to query the cluster, since I already have the cluster set as publicly accessible, and have created an inbound rule in the security group for all ip addresses to access it

### Setup
To run this project, you must set up airflow locally, and make sure the bikeshare_dag.py is in the dags folder, and the scripts and operators directories are in the appropriate places. Then you will need to make sure the aws client is set up in terminal so you can set the credentials with an IAM user. Then you will need to set up the redshift cluster, attach a role to it that contains full s3 and redshift access, make sure it is public and that enhanced VPC is selected. Make sure there is a redshift connection variable created in airflow with the name 'redshift', that all the appropriate fields are populated in dwh.cfg, and your scripts, bootstrap file, and source data are in the correct place in s3. Then activate the dag in airflow and run

### Sample Queries & Output
In this repository are several screenshots I've included to showcase the final product of this pipeline. neighborhood_metrics_screenshot 1 & 2 show the execution of the select statement from the insert that loads the neighborhood metrics table with data from the two staging tables. staging_bikeshare_count, staging_bikeshare_sample, and the e-scooter versions are screenshots of top 5 sample queries of my staging tables and counts on the number of records in those tables

### Inspiration
This project was created for the Udacity Data Engineering Nanodegree course, specifically the Capstone project

