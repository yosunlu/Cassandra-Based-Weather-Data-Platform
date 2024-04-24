
# Project overview

NOAA (National Oceanic and Atmospheric Administration) collects weather data from all over the world. The target of this project is to: 
(1) store this data in Cassandra  
(2) write a server for data collection, and 
(3) analyze the collected data via Spark.

# Cluster Setup

'setup.sh' is provided to download and setup all the necessary files for this project. Note that you might need to give setup.sh executable permission before running it. You can do this running:  
`wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/setup.sh -O setup.sh
chmod u+x setup.sh
./setup.sh`

- run `docker build . -t p6-base`
  - Dockerfile: built on ubuntu:22.04 and wget Cassandra and Spark
- run `docker compose up -d`
  - docker-compose.yml:  
    p6-db-1:   
    - starts three containers 
    - ports: "127.0.0.1:5000:5000" in the first container indicates that, port 5000 in the container will only listen to port 5000 on your local machine 
    - volumes: "./nb:/nb" maps local nb directory to the one in the container
    - note that the first container will run the cassandra script, essentially connecting the three containers; the first container also runs jupyterlab on port 5000, and exposes it to the host machine

Now enter "127.0.0.1:5000" at your browser. This should be the jupyterlab page. Note that occasionally you'll need to clear the cache for the page the appear.


# Files
Source code are stored in ./nb
- p6.ipynb: the main code accomplishes the 10 tasks below
- server.pu: gRPC-based server 
- ghcnd-stations.txt: includes the name and ID of the weather stations (gitignored)
- records.parquet: includes the weather data (gitignored)

# Tasks
There are 10 tasks I wanted to achieve, devided into 4 parts. Corresponding code can be found in p6.ipynb

## Part 1
Moves the station data from ghcnd-stations.txt to a cassandra table via Spark
- Task 1: determine the schema of the created cassandra table
    - Step 1: Connect to the Cassandra cluster
    - Step 2: Create a weather keyspace with 3x replication
    - Step 3: Create the station table
- Task 2: Validate the correctness of the station table
    - Step 1: Create a Spark session. Note that all workers (driver and executors) are running in a single machine (fine for testing/development, but misses the benefits of distributed computing)
    - Step 2: Read the input file, filter results to the state of Wisconsin, and collect the rows
    - Step 3: Iterate the spark data frame, and store ID and Name to cassandra. This step is essentially moving data from spark to cassandra
- Task 3: Check the token for the USC00470273 station
    - Each Cassandra row will be stored in 3 nodes (workers)
- Task 4: Check first vnode token in the ring following the token for USC00470273

## Part 2
Writes a gRPC-based server that that receives temperature data and records it to weather.stations. You could imagine various sensor devices acting as clients that make gRPC calls to server.py to record data, but for simplicity I'll make the client calls from p6.ipynb.  
In server.py, I implemented the interface from station_pb2_grpc.StationServicer. RecordTemps will insert new temperature highs/lows to weather.stations. StationMax will return the maximum tmax ever seen for the given station.  
- Task 5: Create a gRPC-based server. Use this notebook as client that calls the server, and fill in the cassandra table; validate the max temperature ever seen for station USW00014837
    - Both client and server are connected to the cassandra clusters. The weather data are stored at client locally. For each row of data, client will call the gRPC server. The server will store the corresponding weather data to the cassandra station table 
    - The RF(replication factor) is set to 3, W is set to 1, R is set to 3
        - Each Cassandra row will be stored in 3 nodes (workers); W is low because I prioritize high write availability. R is set to 3 so R + W > RF, which allows written nodes and nodes to read from to overlap



## p6.ipynb:
- Table weather.stations is created at the beginning of the notebook
- Use spark to read data from "ghcnd-stations.txt", and moves data (only id and name) to the cassandra table (q1~q4)
- Another file (record.parquet) is read using spark. Use gRPC (where connection to the cassandra table is also made) to insert the dates and records (q5)
- q5 loops the new parquet file (which is converted to list), and pass id, dates, and records to requests

## server.py:
- Implements the interface from station_pb2_grpc.StationServicer
- RecordTemps will insert new temperature highs/lows to weather.stations
- StationMax will return the maximum tmax ever seen for the given station

## why use a cassandra table?
weather.stations table looks like this:

| id          | date     | station_record  |
|-------------|----------|-----------------|
| ACW00011604 | 20200201 | {0, 100}        |
|             | 20200202 | {-1, 99}        |
|             | 20200203 | {-3, 101}       |

- PRIMARY KEY ((id), date): each id (parition key) and date(cluster key) will correspond to a unique row
- Each row is a partition. Use cassandra to ensure each partition is stored in one machine
- The table created in notebook does not have date and station_record
- Use RecordTemps (given id, date, station_record) to fill in the table
- Use StationMax to iterate/sort the station_record to find the largest tmax ever for this station

