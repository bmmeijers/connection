from connect import ConnectionFactory
from random import randint
import warnings
import logging
log = logging.getLogger(__name__)

warnings.warn("Inefficient DB connection in use")


def recordset(sql, parameters = None):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    #
    # might not be very efficient, but leads to a bit more readable code
    _factory = ConnectionFactory()
    _CONN0 = _factory.connection()    
    cursor = _CONN0.cursor()
    if parameters:
        cursor.execute(sql, parameters)
    else:
        cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    _CONN0.commit()
    _CONN0.close()
    del cursor
    return rows

def irecordset(sql, parameters = None, size = 2500):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    #
    # might not be very efficient, but leads to a bit more readable code
    _factory = ConnectionFactory()
    CONN = _factory.connection()    
    cursor = CONN.cursor('named_{0}'.format(randint(1, 1e9)))
    if parameters:
        cursor.execute(sql, parameters)
    else:
        cursor.execute(sql)
    while True:
        rows = cursor.fetchmany(size)
        if not rows:
            break
        for row in rows:
            yield row
    cursor.close()
    CONN.commit()
    CONN.close()
    del rows
    del cursor

def record(sql, parameters = None):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    #
    # might not be very efficient, but leads to a bit more readable code
    _factory = ConnectionFactory()
    CONN = _factory.connection()    
    cursor = CONN.cursor()
    try:
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
    except:
        print sql
        raise

    one = cursor.fetchone()

    cursor.close()
#    CONN.commit()
    del cursor
    return one

def execute(sql, parameters = None):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    #
    # might not be very efficient, but leads to a bit more readable code
    _factory = ConnectionFactory()
    CONN = _factory.connection()    
    cursor = CONN.cursor()
    try:
        log.debug(sql)
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
    except:
        raise
    CONN.commit()
    cursor.close()
    del cursor
