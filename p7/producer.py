from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import report_pb2
import weather

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=broker)
topic = "temperatures"

try:
    admin_client.delete_topics([topic])
except Exception as e:
    print(f"Topic not found for deletion: {e}")

new_topic = NewTopic(name=topic, num_partitions=4, replication_factor=1)
try:
    admin_client.create_topics([new_topic])
except Exception as e:
    print(f"Error creating topic: {e}")

producer = KafkaProducer(
    bootstrap_servers=broker,
    retries=10,
    acks='all'
)

for date, degrees, station_id in weather.get_next_weather(delay_sec=0.1):
    report = report_pb2.Report(date=date, degrees=degrees, station_id=station_id)
    producer.send(
        topic,
        key=station_id.encode('utf-8'),
        value=report.SerializeToString()
    )
    producer.flush()
