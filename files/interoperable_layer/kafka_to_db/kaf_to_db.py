import sqlite3
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import logging
from concurrent.futures import ThreadPoolExecutor
import sys

logging.basicConfig(level=logging.INFO, filename='kaf_to_db.txt', filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KafkaSQLiteConsumer:
    """
    A class to consume messages from Kafka and store them in SQLite database.

    Attributes:
        db_file (str): The SQLite database file path.
        kafka_servers (str): The Kafka bootstrap servers.
        num_threads (int): The number of threads for concurrent consumers.
    """

    def __init__(self, db_file, kafka_servers, topic, num_threads=5):
        """
        Initialize the KafkaSQLiteConsumer.

        Args:
            db_file (str): The SQLite database file path.
            kafka_servers (str): The Kafka bootstrap servers.
            num_threads (int, optional): The number of threads for concurrent consumers. Default is 5.
        """
        self.db_file = db_file
        self.kafka_servers = kafka_servers
        self.num_threads = num_threads
        self.topic = topic

    def create_database(self, fields):
        """
        Create the SQLite database table if it does not exist.
        """
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            create_query = f"CREATE TABLE IF NOT EXISTS {self.topic} ("
            create_query += ", ".join([f"{field} TEXT" for field in fields])
            create_query += ")"
            c.execute(create_query)
            conn.commit()
            logger.info("Table created in database")
        except sqlite3.Error as e:
            logger.error(f"Error creating database table: {e}")
            sys.exit(1)
        finally:
            conn.close()

    def insert_data(self, data):
        """
        Insert data into the SQLite database.

        Args:
            data (list): A list of dictionaries containing message data.
        """
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            for entry in data:
                insert_query = f"INSERT INTO {self.topic} ({', '.join(entry.keys())}) VALUES ({', '.join(['?' for _ in entry.keys()])})"
                c.execute(insert_query, list(entry.values()))
            conn.commit()
            logger.info("Data saved to database: %s", data)
        except sqlite3.Error as e:
            logger.error(f"Error inserting data into database: {e}")
        finally:
            conn.close()

    def process_and_store(self, messages):
        """
        Process and store messages from Kafka.

        Args:
            messages (dict): A dictionary containing Kafka messages.
        """
        logger.info("Received messages from Kafka")
        data = [json.loads(msg.value.decode('utf-8')) for msg in messages]
        current_time = datetime.now()
        one_hour_ago = current_time - timedelta(hours=1)
        recent_hour_data = [entry for entry in data if datetime.strptime(entry['time'], '%H:%M:%S') < one_hour_ago]
        self.store_all_time_data(data)

        with open('fresh_data.json', 'w') as f:
            json.dump(recent_hour_data, f)
        logger.info("Fresh data stored: %s", recent_hour_data)

    def store_all_time_data(self, data):
        """
        Store all-time data into the SQLite database.

        Args:
            data (list): A list of dictionaries containing message data.
        """
        self.create_database(data[0].keys())
        self.insert_data(data)

    def consume_and_store(self, consumer_group, topic):
        """
        Consume messages from Kafka and store them.

        Args:
            consumer_group (str): The Kafka consumer group.
            topic (str): The Kafka topic to consume from.
        """
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_servers)

            existing_topics = admin_client.list_topics()
            if topic not in existing_topics:
                new_topic = NewTopic(name=topic, num_partitions=1, replication_factor=1)
                admin_client.create_topics(new_topics=[new_topic])
                logger.info(f"Created topic '{topic}'")

            consumers = []
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                for _ in range(self.num_threads):
                    consumer = KafkaConsumer(bootstrap_servers=self.kafka_servers, group_id=consumer_group)
                    partitions = consumer.partitions_for_topic(topic)
                    if partitions is None:
                        raise ValueError(f"Topic '{topic}' does not exist or has no partitions.")
                    consumer.assign([TopicPartition(topic, p) for p in partitions])
                    consumers.append(consumer)
                    executor.submit(self.consume_and_process, consumer)
            
            for consumer in consumers:
                consumer.close()
            logger.info("All consumers stopped")
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"An error occurred: {e}")

    def consume_and_process(self, consumer):
        """
        Consume messages from Kafka and process them.

        Args:
            consumer (KafkaConsumer): The Kafka consumer instance.
        """
        while True:
            messages = consumer.poll(timeout_ms=1000, max_records=100)  # Adjust max_records according to performance needs
            for partition, records in messages.items():  
                 self.process_and_store(records)  
        # except Exception as e:
       
import sys
if __name__ == "__main__":
    db_file = 'sensor_data.db'
    kafka_servers = 'localhost:9092'
    consumer_group = 'sensor-consumer-group'
    topic = sys.argv[1] if len(sys.argv) > 1 else 'temperature'
    num_threads = 5
    
    kafka_sqlite_consumer = KafkaSQLiteConsumer(db_file, kafka_servers, topic, num_threads)
    kafka_sqlite_consumer.consume_and_store(consumer_group, topic)
