# Kafka -> Spark -> Cassandra Processing Example

Run locally:   
1. Start dependencies `docker-compose up`  
2. Start the `cassandra_streaming_dag` job  
3. Start the `kafka_streaming_dag` job  
4. The data should be placed from API to Kafka and then moved to Cassandra  

Tools:  
* Airflow UI: http://localhost:8080/home  
* Spark UI: http://localhost:9090
