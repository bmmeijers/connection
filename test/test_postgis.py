import sys
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s (%(process)d) %(message)s', 
    stream=sys.stdout, 
    level=logging.DEBUG)

from connection import Connection

conn = Connection.connection()

sql = "SELECT postgis_full_version()"
for item, in conn.recordset(sql): 
    print item
