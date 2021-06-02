import sys
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s (%(process)d) %(message)s', 
    stream=sys.stdout, 
    level=logging.DEBUG)

from connection import connection

with connection() as db:
    sql = "SELECT version()"
    for item, in db.recordset(sql): 
        print(item)
    
    sql = "SELECT postgis_full_version()"
    for item, in db.recordset(sql): 
        print(item)
