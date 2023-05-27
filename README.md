## Running the docker

To get started run ``` docker-compose up ``` in root directory.
It will create the PostgresSQL database and start generating the data.
It will create an empty MySQL database.
It will launch the analytics.py script. 

Your task will be to write the ETL script inside the analytics/analytics.py file.

## Alternative Solutions:
- Solution 1: instead of using pandas, i can write sql queries directly to select and aggregate required data points
- Solution 2: for more relaiability i would have chosen the kafka approach to source from postgres and sink into mysql