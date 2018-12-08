# How to run

## Setup the environment:
**To setup the environment, you need to have docker installed.**

  
First clone the project 
```
git clone https://github.com/MohammadMazraeh/movielens-recommender.git
```
Then go to docker=-compose folder by ``cd docker-compose``.  
Start Kafka, Zookeeper, Kafka-Manager, Elasticsearch, Kibana and Cassandra by running the following commands respectively.

```
docker-compose -f elasticsearch.yml up -d
docker-compose -f kafka.yml up -d
docker-compose -f cassandra.yml up -d
```
\* Make sure to set vm.max_map_count on system by following the instructions from here: [Set Virtual Memory  in Linux][Elasticsearch System Configuration]

[Elasticsearch System Configuration]: https://www.elastic.co/guide/en/elasticsearch/reference/current/vm-max-map-count.html

## Download dataset
Go to `dataset` and download the dataset by running the below command. It would download an small movie lens dataset and extract it. feel free to download a bigger dataset.
```
bash download_dataset.sh
```
## Prepare the data:
First set the below parameters according to your environment:
You need to set path for **Spark 2.4** and **Python 3.5+**.

```
export SPARK_HOME=/home/ubuntu/spark-2.4.0-bin-hadoop2.7
export PYSPARK_PYTHON=python3
```

Go to `data_loading` folder. Run the following commands to load the necessary data into cassandra and Kafka.
You need to change the ip address of Cassandra and Kafka in the files.
```
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 data_loading/batch_load.py
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 data_loading/batch_load2.py
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 data_loading/event_stream_generator.py
```

## Training the model

In the main folder run:
```
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11 recommender/train_batch.py
```
It would read the data from cassandra and train an ALS model. then it stores it in **models/ALS**.

## Testing the Model

To test the model after changing the IPs in `recommender/stream_predictor.py`, start it by running:
```
spark-submit --packages datastax:spark-cassandra-connector:2.3.1-s_2.11,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 recommender/stream_predictor.py
```
It would read the get requests from `kafka_input_topic` topic with a limited rate from Kafka and writes the result back to Kafka in `kafka_output_topic`. 

## Write prediction result to elasticsearch

To read data from kafka and write it into elasticsearch, I've used a logstash docker image.

Go to `docker-compose` folder and edit `logstash-pipelines/predictions.conf` as necessary (Change Ip addresses of Kafka and Elasticsearch)
then run `docker-compose -f logstash.yml up -d`. It would start a job and read the data from Kafka, convert them to jsons and write to Elasticsearch.

## Visualizing

Go to `SERVER_ADDRESS:5601` and create the charts by selecting the chart type and the fields. There are several pictures from dashboard available in `documents/dashboard-images`.

## List of services and ports
```
Kafka: 9092  
Zookeeper: 2181
Kafka Manager: 9000  
Elasticsearch: 9200
Kibana: 5601 
```
