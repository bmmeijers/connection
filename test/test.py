import sys
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s (%(process)d) %(message)s', 
    stream=sys.stdout, 
    level=logging.DEBUG)

from connection.stateful import recordset

sql = "SELECT postgis_full_version()"
for item, in recordset(sql): 
    print item