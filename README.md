# AWS Sparkify Database and ETL

## What is Sparkify?

Sparkify, a music streaming startup, wanted to collect logs they have on user activity and song data and centralize them in a database in order to run analytics. This Redshift database, set up with a star schema, will help them to easily access their data in an intuitive fashion and start getting rich insights into their user base.

## Why this Database and ETL design?

My client Sparkify has moved to a cloud based system and now keeps their big data logs in an AWS S3 bucket. The end goal was to get that raw .json data from their logs into fact and dimenstion tables on a Redshift data warehouse. I opted to use an intermediary staging table for both the raw song and log data in order to provide more efficient database transformations.

Our fact table "songplays" contains foreign keys to our dimension table. Our dimension tables contain the descriptive elements like times, durations and other measurements of our data.

## Database structure overview

![ER Diagram](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/38715/1607614393/Song_ERD.png)
*From Udacity*

## How to run

- Start by cloning this repository
- Install all python requirements from the requirements.txt
- In the `launch_redshift.py` script run the `setup_iam_roles()` function to create an IAM role
- Fill in the dwh_template with your own custom details
- To spin up and then delete the redshift cluster run the functions `create_redshift_cluster` and `delete_redshift_cluster` respectively in `launch_redshift.py`
- Run `python create_tables.py` to initialize the database and its tables
- Run `python etl.py` to load all the json data into the staging and analytics tables

## Example song analysis queries

Find area with highest amount of listening instances:  
`SELECT location, count(location) as num_of_listeners FROM songplays group by location order by num_of_listeners desc limit 1;`  
Result: San Francisco-Oakland-Hayward, CA, 691

Find amount of paying users:  
`SELECT count(level) FROM users WHERE level = 'paid';`  
Result: 5591
