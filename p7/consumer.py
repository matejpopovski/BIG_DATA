import sys
import os
import json
from kafka import KafkaConsumer, TopicPartition
import report_pb2

def write_json_atomically(file_path, data):
    temp_file = f"{file_path}.tmp"
    with open(temp_file, "w") as f:
        json.dump(data, f)
    os.rename(temp_file, file_path)

def process_message(message, stats):
    report = report_pb2.Report()
    report.ParseFromString(message.value)
    station_id = report.station_id
    date = report.date
    degrees = report.degrees

    if station_id not in stats:
        stats[station_id] = {
            "count": 0,
            "sum": 0.0,
            "avg": 0.0,
            "start": date,
            "end": date
        }
    stats[station_id]["count"] += 1
    stats[station_id]["sum"] += degrees
    stats[station_id]["avg"] = stats[station_id]["sum"] / stats[station_id]["count"]
    stats[station_id]["start"] = min(stats[station_id]["start"], date)
    stats[station_id]["end"] = max(stats[station_id]["end"], date)

def main():
    if len(sys.argv) < 2:
        print("Usage: consumer.py <partition1> <partition2> ...")
        return

    partitions = list(map(int, sys.argv[1:]))
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092',
        group_id=None,
        auto_offset_reset='earliest'
    )
    topic = 'temperatures'
    assigned_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(assigned_partitions)

    print(f"DEBUG: Assigned partitions: {assigned_partitions}") # All Debug Statements provided by ChatGPT. 

    stats = {}
    for tp in assigned_partitions:
        offset_file = f"/src/partition-{tp.partition}.json"
        print(f"DEBUG: Checking offset file: {offset_file}")
        if os.path.exists(offset_file):
            print(f"DEBUG: Offset file exists: {offset_file}")
            with open(offset_file) as f:
                data = json.load(f)
                stats.update({k: v for k, v in data.items() if k != "offset"})
                consumer.seek(tp, data.get("offset", 0))
                print(f"DEBUG: Seeking to offset {data.get('offset', 0)} for partition {tp.partition}")
        else:
            print(f"DEBUG: No offset file for partition {tp.partition}, seeking to beginning")
            consumer.seek_to_beginning(tp)

    for message in consumer:
        print(f"DEBUG: Received message from partition {message.partition}: {message.value}") 
        process_message(message, stats)
        partition_file = f"/src/partition-{message.partition}.json"
        tp = TopicPartition(topic=message.topic, partition=message.partition)
        stats["offset"] = consumer.position(tp)
        print(f"DEBUG: Writing stats to {partition_file}: {stats}")
        write_json_atomically(partition_file, stats)

if __name__ == "__main__":
    main()