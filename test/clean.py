from connection import connection
sql = """
SELECT * FROM pg_catalog.pg_tables 
WHERE tablename LIKE 'qt_%' 
ORDER BY tablename;
"""
pgconn = connection(True)
with open("/home/martijn/tmp/clean.sql", "w") as fh:
    for schema, table, owner, x,y,z,w in pgconn.irecordset(sql):
        if schema == "martijn" and owner == "martijn":
            print >> fh, "DROP TABLE IF EXISTS " + table + " CASCADE;"
