
# Project overview

## p6.ipynb:
- Table weather.stations is created at the beginning of the notebook
- Use spark to read data from "ghcnd-stations.txt", and moves data (only id and name) to the cassandra table (q1~q4)
- Another file (record.parquet) is read using spark. Use gRPC (where connection to the cassandra table is also made) to insert the dates and records (q5)
- Why use cassandra for the table?
- weather.stations table looks like this:

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

## server.py:
- Implements the interface from station_pb2_grpc.StationServicer
- RecordTemps will insert new temperature highs/lows to weather.stations
- StationMax will return the maximum tmax ever seen for the given station

