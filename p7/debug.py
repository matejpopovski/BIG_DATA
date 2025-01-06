from kafka import KafkaConsumer
import report_pb2

consumer = KafkaConsumer(
    'temperatures',
    bootstrap_servers='localhost:9092',
    group_id='debug',
    auto_offset_reset='latest'
)

for message in consumer:
    report = report_pb2.Report()
    try:
        report.ParseFromString(message.value)
        print({
            "station_id": report.station_id,
            "date": report.date,
            "degrees": report.degrees,
            "partition": message.partition
        })
    except Exception as e:
        print(f"Failed to parse message: {e}")