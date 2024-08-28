import logging
import uuid

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """
    )
    print("Keyspace created successfully!")


def create_table(session):
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
        """
    )
    print("Table created successfully!")


def insert_data(session, data):
    print("inserting data...")

    try:
        session.execute(
            """
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                data["id"],
                data["first_name"],
                data["last_name"],
                data["gender"],
                data["address"],
                data["post_code"],
                data["email"],
                data["username"],
                data["registered_date"],
                data["phone"],
                data["picture"],
            ),
        )
        print(f"Data inserted for user {data['id']}")
    except Exception as e:
        print(f"could not insert data due to {e}")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(s_conn):
    result_df = None
    try:
        result_df = (
            s_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "users_created")
            .option("startingOffsets", "earliest")
            .load()
        )
        print("kafka dataframe created successfully")
    except Exception as e:
        print(f"kafka dataframe could not be created because: {e}")
    return result_df


def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        print(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(df):
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False),
        ]
    )
    result_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    print("Schema applied to Kafka stream")
    return result_df


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        kafka_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(kafka_df)
        cassandra_conn = create_cassandra_connection()

        if cassandra_conn is not None:
            create_keyspace(cassandra_conn)
            create_table(cassandra_conn)

            print("Streaming is being started...")

            # streaming_query = (
            #     selection_df.writeStream.format("org.apache.spark.sql.cassandra")
            #     .option("checkpointLocation", "/tmp/checkpoint")
            #     .option("keyspace", "spark_streams")
            #     .option("table", "created_users")
            #     .option(
            #         "spark.cassandra.connection.host", "localhost"
            #     )  # Make sure the Cassandra host is specified
            #     .option(
            #         "spark.cassandra.connection.port", "9042"
            #     )  # Explicitly specify the port
            #     .start()
            # )

            def process_batch(df, epoch_id):
                def insert_row(row):
                    # Extract data from the row
                    data = {
                        "id": row.id,
                        "first_name": row.first_name,
                        "last_name": row.last_name,
                        "gender": row.gender,
                        "address": row.address,
                        "post_code": row.post_code,
                        "email": row.email,
                        "username": row.username,
                        "registered_date": row.registered_date,
                        "phone": row.phone,
                        "picture": row.picture,
                    }

                    # Insert data into Cassandra
                    insert_data(cassandra_conn, data)

                if not df.isEmpty():
                    df.foreach(insert_row)
                else:
                    print("No records in this batch")

            streaming_query = (
                selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                .option("checkpointLocation", "/tmp/checkpoint")
                .option("keyspace", "spark_streams")
                .option("table", "created_users")
                .start()
            )

            streaming_query.awaitTermination()
