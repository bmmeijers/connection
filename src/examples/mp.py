import os
from connection.dumb import ConnectionFactory
from multiprocessing import Process, Queue

# Multiprocessing example

class Worker(Process):
    def __init__(self, queue):
        super(Worker, self).__init__()
        self.queue = queue

    def run(self):
        print 'Worker started'
        # we connect to the DBMS here, because this needs to be done
        # after spawning the Python child process
        self.connect()
        for data in iter(self.queue.get, None):
            # Use data
            print data, os.getpid()
#            c = conn0.cursor()
#            c.execute('select face_id from atkis_face')
            for fid, mbr, in self.conn0.irecordset('select face_id, mbr_geometry from atkis_face'):
                print os.getpid(), fid, mbr
                for eid, in self.conn1.irecordset('select edge_id from atkis_edge where left_face_id in ({0}) or right_face_id in ({0})'.format(fid)):
                    print "", eid
        self.conn0.close()
        return
    
    def connect(self):
        # do some initialization here
        self.conn0 = ConnectionFactory.connection(True)
        self.conn1 = ConnectionFactory.connection(True)

request_queue = Queue()
for i in range(3):
    Worker( request_queue ).start()

for data in range(3):
    request_queue.put( data )

# Sentinel objects to allow clean shutdown: 1 per worker.
for i in range(4):
    request_queue.put( None )
