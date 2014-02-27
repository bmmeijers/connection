from connection import Connection
conn = Connection.connection()
cursor = conn._conn.cursor()
cursor.execute("SELECT NULL::box2d")
box2d_oid = cursor.description[0][1]
cursor.close()
print box2d_oid