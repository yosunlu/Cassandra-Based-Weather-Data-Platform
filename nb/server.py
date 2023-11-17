from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from concurrent import futures
import station_pb2_grpc
import station_pb2
import grpc


cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
cass = cluster.connect()
cass.execute("USE weather")

class Record(object):

    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax

cluster.register_user_type('weather', 'station_record', Record)

class ModelServer(station_pb2_grpc.StationServicer):
    def __init__(self):
            self.insert_statement = cass.prepare("""
            INSERT INTO stations (id, date, record)
            VALUES(?, ?, ?)
            """)
            self.insert_statement.consistency_level = ConsistencyLevel.ONE

            self.max_statement = cass.prepare("""
            SELECT record.tmax
            FROM stations
            WHERE id = ?
            """)
            self.max_statement.consistency_level = ConsistencyLevel.THREE

    def RecordTemps(self, request, context):
        try:
            cass.execute(self.insert_statement, (request.station, request.date, Record(request.tmin, request.tmax)))
            return station_pb2.RecordTempsReply(error="") 

        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        try:
            result = cass.execute(max_statement, request.station)
            row = result.one()
            return station_pb2.StationMaxReply(tmax=row.tmax, error="")
        except Exception as e:
            return station_pb2.StationMaxReply(error=str(e))

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    station_pb2_grpc.add_StationServicer_to_server(ModelServer(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    print("Server started on port 5440...")
    server.wait_for_termination()
