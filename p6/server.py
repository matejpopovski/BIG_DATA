import grpc
from concurrent import futures
from cassandra.cluster import Cluster
from cassandra import Unavailable, cluster
from cassandra.query import ConsistencyLevel
from pyspark.sql import SparkSession
from datetime import datetime
import station_pb2
import station_pb2_grpc

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.session = cluster.connect()

        self.session.execute("DROP KEYSPACE IF EXISTS weather")
        self.session.execute("""
            CREATE KEYSPACE weather WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 3
            }
        """)

        self.session.set_keyspace('weather')

        self.session.execute("""
            CREATE TYPE IF NOT EXISTS station_record (
                tmin int,
                tmax int
            )
        """)

        self.session.execute("""
            CREATE TABLE IF NOT EXISTS stations (
                id text,
                name text static,
                date date,
                record station_record,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """)

        

        self.spark = SparkSession.builder.appName("p6").getOrCreate()

        file_path = "/src/ghcnd-stations.txt"
        stations_df = self.spark.read.text(file_path)

        stations_df = stations_df.select(
        stations_df.value.substr(1, 11).alias("id"),
        stations_df.value.substr(39, 2).alias("state"),
        stations_df.value.substr(42, 30).alias("name")
        )
        wi_stations = stations_df.filter(stations_df.state == "WI").collect()

        for row in wi_stations:
            self.session.execute(
                "INSERT INTO stations (id, name) VALUES (%s, %s)",
                (row.id.strip(), row.name.strip())
            )


        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!


    def StationSchema(self, request, context):
        try:
            metadata = self.session.cluster.metadata
            table_metadata = metadata.keyspaces['weather'].tables['stations']
            
            create_statement = table_metadata.as_cql_query()
            
            return station_pb2.StationSchemaReply(schema=create_statement, error="")
        except Exception as e:
            return station_pb2.StationSchemaReply(schema="", error=str(e))
    
    def StationName(self, request, context):
        try:
            result = self.session.execute(
                "SELECT name FROM stations WHERE id = %s",
                (request.station,)
            ).one()

            if result and result.name:
                return station_pb2.StationNameReply(name=result.name.strip(), error="")
            else:
                return station_pb2.StationNameReply(name="", error="Station ID not found")
        except Exception as e:
            return station_pb2.StationNameReply(name="", error=str(e))

    def RecordTemps(self, request, context):
        try:
            insert_cql = self.session.prepare('''
            INSERT INTO weather.stations (id, date, record)
            VALUES (?, ?, {tmin:?, tmax:?})
            ''')
            insert_cql.consistency_level = ConsistencyLevel.ONE

            record = {"tmin": request.tmin, "tmax": request.tmax}

            self.session.execute(
            insert_cql,
            (request.station, request.date, request.tmin, request.tmax)
            )
            return station_pb2.RecordTempsReply(error="")

        except (Unavailable, cluster.NoHostAvailable):
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))
    
    def StationMax(self, request, context):
        try:
            max_cql = self.session.prepare('''
            SELECT MAX(record.tmax) AS max_tmax
            FROM weather.stations
            WHERE id = ?
            ''')
            max_cql.consistency_level = ConsistencyLevel.THREE 

            result = self.session.execute(max_cql, (request.station,)).one()

            if result and result.max_tmax is not None:
                return station_pb2.StationMaxReply(tmax=result.max_tmax, error="")
            else:
                return station_pb2.StationMaxReply(tmax=-1, error="No data found for the station")
        except (Unavailable, cluster.NoHostAvailable):
            return station_pb2.StationMaxReply(tmax=-1, error="unavailable")
        except Exception as e:
            return station_pb2.StationMaxReply(tmax=-1, error=str(e))

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
