# Project 3: Song Play Analysis with S3 and Redshift


### Introduction

In this project, We are building an pipeline to the ETL processes for a streaming startup call SPARKIFY. We use AWS services such as S3 to stage to transform and finally load them into redshift datawarehouse. The purposes of this project is to help the analytic teams have better access to the data.ds

### Datasets
There are two folder of datasets for this project. First folder contains song and information of that particular song stored as JSON format. The second folders contains log datasets, which give information for time and duration of each song played.
.

### Database Schema
We have two staging tables which *copy* the JSON file inside the  **S3 buckets**.
#### Staging Table 
+ **staging_songs** - info about songs and artists
+ **staging_events** - User activity table

#### Fact Table 
+ **songplays** - records in event data associated with song plays 

#### Dimension Tables
+ **users** - users in the app
+ **songs** - songs in music database
+ **artists** - artists in music database
+ **time** - timestamps information from **songplays** but grinded down into specific units


### Data Warehouse Configurations and Setup
* Create a new `IAM user` in your AWS account
* Give it AdministratorAccess and Attach policies
* Use access key and secret key to create clients for `EC2`, `S3`, `IAM`, and `Redshift`.
* Create an `IAM Role` that makes `Redshift` able to access `S3 bucket` (ReadOnly)
* Create a `RedShift Cluster` and get the `DWH_ENDPOIN(Host address)` and `DWH_ROLE_ARN` and fill the config file.

### ETL Pipeline
+ Created tables to store the data from `S3 buckets`.
+ Loading the data from `S3 buckets` to staging tables in the `Redshift Cluster`.
+ Inserted data into fact and dimension tables from the staging tables.

### Project Structure

+ `create_tables.py` - This script will drop old tables re-create new tables.
+ `etl.py` - This script executes the queries that extract `JSON` data from the `S3 bucket` and ingest them to `Redshift`.
+ `sql_queries.py` -  variables with SQL statement in String formats, partitioned by `CREATE`, `DROP`, `COPY` and `INSERT` statement.
+ `dhw.cfg` - Configuration file used that contains info about `Redshift`, `IAM` and `S3`

### How to Run

1. Create tables by running `create_tables.py`.

2. Execute ETL process by running `etl.py`.








{"mode":"full","isActive":false}