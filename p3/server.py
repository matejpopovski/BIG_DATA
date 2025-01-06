import csv
import os
import threading
import uuid
from concurrent import futures

import grpc
import pandas as pd
import pyarrow.csv as pycsv
import pyarrow.parquet as pq
import table_pb2
import table_pb2_grpc

CSV_DIRECTORY = "./csv_files"
PARQUET_DIRECTORY = "./parquet_files"
lock = threading.Lock()
file_records = []

class TableServicer(table_pb2_grpc.TableServicer):
    def Upload(self, request, context):
        try:
            unique_id = uuid.uuid4().hex
            csv_file_path = os.path.join(CSV_DIRECTORY, f"file_{unique_id}.csv")
            parquet_file_path = os.path.join(PARQUET_DIRECTORY, f"file_{unique_id}.parquet")
            
            with open(csv_file_path, "wb") as csv_file:
                csv_file.write(request.csv_data)

            data_table = pycsv.read_csv(csv_file_path)
            pq.write_table(data_table, parquet_file_path)

            with lock:
                file_records.append({"csv": csv_file_path, "parquet": parquet_file_path})

            return table_pb2.UploadResp()

        except Exception as error:
            return table_pb2.UploadResp(error=str(error))

    def ColSum(self, request, context):
        column_sum = 0.0
        try:
            with lock:
                file_paths = [record[request.format] for record in file_records]

            for file_path in file_paths:
                if request.format == "csv":
                    try:
                        csv_data = pd.read_csv(file_path, usecols=[request.column])
                        column_sum += csv_data[request.column].dropna().astype(float).sum()
                    except Exception:
                        continue

                elif request.format == "parquet":
                    try:
                        data_table = pq.read_table(file_path, columns=[request.column])
                        column_data = data_table[request.column].to_pandas()
                        column_sum += column_data.dropna().astype(float).sum()
                    except (KeyError, ValueError):
                        continue

            return table_pb2.ColSumResp(total=int(column_sum))

        except Exception as error:
            return table_pb2.ColSumResp(error=str(error))

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    table_pb2_grpc.add_TableServicer_to_server(TableServicer(), server)
    server.add_insecure_port("[::]:5440")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    if not os.path.exists(CSV_DIRECTORY):
        os.makedirs(CSV_DIRECTORY)
    if not os.path.exists(PARQUET_DIRECTORY):
        os.makedirs(PARQUET_DIRECTORY)
    
    serve()
