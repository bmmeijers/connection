#!/usr/bin/python
import psycopg2
import sys

con = None
query = ''

try:
    if len(sys.argv) == 6:
      
        host = sys.argv[1]
        database = sys.argv[2]
        user = sys.argv[3]
        password = sys.argv[4]
        days = sys.argv[5]
        
        con = psycopg2.connect(host=host, database=database, user=user, password=password) 
        cur = con.cursor()
        cur.execute('SELECT relname ' + 
                    'FROM pg_stat_all_tables ' +
                    "WHERE schemaname = 'public' " +
                    'AND ((last_analyze is NULL ' +
                    'AND last_autoanalyze is NULL)' +
                    'OR ((last_analyze < last_autoanalyze OR last_analyze is null) ' +
                    "AND last_autoanalyze < now() - interval %s) " +
                    'OR ((last_autoanalyze < last_analyze OR last_autoanalyze is null) ' +
                    "AND last_analyze < now() - interval %s));", [days + ' day', days + ' day'])
        rows = cur.fetchall()
        con.set_isolation_level(0)
        for row in rows:
            query = 'VACUUM ANALYZE %s;' % (row[0])
            cur.execute(query)
    else:
        print 'This script needs 5 arguments [host] [database] [user] [password] [days]'

except psycopg2.DatabaseError, e:
    print 'Error: %s' % e    
    sys.exit(1)
    
finally:
    if con:
        con.close()