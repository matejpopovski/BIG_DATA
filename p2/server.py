import socket
import sys
from concurrent import futures

import grpc
import pandas as pd

from matchdb_pb2 import GetMatchCountResp
from matchdb_pb2_grpc import (MatchCountServicer,
                              add_MatchCountServicer_to_server)


def read_wins_by_parts():
    ip_contain = socket.gethostbyname(socket.gethostname())
    ip_1 = socket.gethostbyname("wins-server-1")
    ip_2 = socket.gethostbyname("wins-server-2")
    if ip_contain == ip_1:
        return "partitions/part_{}.csv".format(0)
    elif ip_contain == ip_2:
        return "partitions/part_{}.csv".format(1)


class MatchCountService(MatchCountServicer):
    def __init__(self, data):
        self.data = data

    def GetMatchCount(self, request, context):
        df = self.data.copy()
        df ["winning_team"] = df["winning_team"].fillna("").astype(str).str.strip()
        df ["country"] = df["country"].fillna("").astype(str).str.strip()
        if request.country:
            df = df[df["country"] == request.country]
        if request.winning_team:
            df = df [df["winning_team"] == request.winning_team]
        num = len(df)
        return GetMatchCountResp(num_matches=num)


def start_serve(data, port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) #1
    add_MatchCountServicer_to_server(MatchCountService(data), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: python server.py <PATH_TO_CSV_FILE> <PORT>")
        csv_path = sys.argv[1]
        port = int(sys.argv[2])
    elif len(sys.argv) > 1:
        print("Usage: python server.py <PATH_TO_CSV_FILE>")
        csv_path = sys.argv[1]
        port = 5440
    else:
        print("Usage: python server.py")
        csv_path = read_wins_by_parts()
        port = 5440

    df_data = pd.read_csv(csv_path)
    start_serve(data=df_data, port=port)
