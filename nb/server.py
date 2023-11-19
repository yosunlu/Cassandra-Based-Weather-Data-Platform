from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from concurrent import futures
import station_pb2_grpc
import station_pb2
import grpc

"""
Implements the interface from station_pb2_grpc.StationServicer
RecordTemps will insert new temperature highs/lows to weather.stations
StationMax will return the maximum tmax ever seen for the given station

"""

cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
cass = cluster.connect()
cass.execute("USE weather")

class Record(object):
    """
    This class helps inserting station_type, which is a user defined typed in notebook:
    cass.execute("CREATE TYPE station_record (tmin int, tmax int)")
    """
    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax

cluster.register_user_type('weather', 'station_record', Record)

class ModelServer(station_pb2_grpc.StationServicer):
    def __init__(self):
            # initilize (prepare) here so the statement can be stored in cached; better for reuse
            self.insert_statement = cass.prepare("""
            INSERT INTO stations (id, date, record)
            VALUES(?, ?, ?) 
            """)
            # ?, ?, ? will correspond to (request.station, request.date, Record(request.tmin, request.tmax)) at line 72
            # Note that W = 1 (ConsistencyLevel.ONE) because we prioritize high write availability
            self.insert_statement.consistency_level = ConsistencyLevel.ONE

            self.max_statement = cass.prepare("""
            SELECT id, record
            FROM stations
            WHERE id = ?
            """)
            self.max_statement.consistency_level = ConsistencyLevel.THREE # Choose R so that R + W > RF

    def RecordTemps(self, request, context):
        """
        Given the request, fill in the weather.stations table
        gRPC fucntion:
        request.
            station: str
            date: str
            tmin: int
            tmax: int
        """
        try:
            # execute the prepare statement
            cass.execute(self.insert_statement, (request.station, request.date, Record(request.tmin, request.tmax)))
            return station_pb2.RecordTempsReply(error="") 

        except Exception as e:
            #TODO: modfiy the error when there's not enough node
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        """
        Given the request (only has id), return the largest tmax ever seen
        """
        try:
            rows = cass.execute(self.max_statement, [request.station]) # turning the df into a list; the argument for ? must be a list, even if there's only one
            sorted_rows = sorted(rows, key=lambda x: x.record.tmax, reverse=True) # sort the list based on tmax
            return station_pb2.StationMaxReply(tmax=sorted_rows[0].record.tmax, error="") # sorted_rows[0] will be the first row of the given station id

        except Exception as e:
            #TODO: modfiy the error when there's not enough node??
            #TODO: We want to avoid a situation where a StationMax returns a smaller temperature than one previously added with RecordTemps
            # it would be better to return an error message if necessary
            return station_pb2.StationMaxReply(error=str(e))

if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    station_pb2_grpc.add_StationServicer_to_server(ModelServer(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    print("Server started on port 5440...")
    server.wait_for_termination()
