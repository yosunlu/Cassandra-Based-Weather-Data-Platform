# Overview

NOAA (National Oceanic and Atmospheric Administration) collects weather data from all over the world. The target of this project is to (1) store this data in Cassandra, (2) write a server for data collection, and (3) analyze the collected data via Spark.

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

Now enter "127.0.0.1:5000" at your browser. This should be the jupyterlab page. Note that occasionally you'll need to clear the cache for the page the appear.

