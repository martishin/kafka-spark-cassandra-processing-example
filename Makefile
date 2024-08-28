submit-job:
	spark-submit \
        --master spark://localhost:7077 \
        --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        jobs/spark_job.py

topic-listen:
	docker exec -it broker kafka-console-consumer --bootstrap-server broker:9092 --topic users_created --from-beginning
