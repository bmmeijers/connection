from random import randint
import logging

from connect import ConnectionFactory

log = logging.getLogger(__name__)
_factory = ConnectionFactory()
# we open 2 connections, so that we can commit without interfering
# server side cursors
_CONN0 = _factory.connection()
_CONN1 = _factory.connection()

#_CURS0 = _CONN0.cursor()
#def srecordset(sql, parameters = None):
#    # takes care of having to write boiler plate code for cursors and
#    # opening database connections
#    #
#    # might not be very efficient, but leads to a bit more readable code
#    if parameters:
#        _CURS0.execute(sql, parameters)
#    else:
#        _CURS0.execute(sql)
#    rows = _CURS0.fetchall()
#    return rows

def recordset(sql, parameters = None):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    cursor = _CONN0.cursor()
    if parameters:
        cursor.execute(sql, parameters)
    else:
        cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    del cursor
    return rows

def irecordset(sql, parameters = None, size = 2500):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    # This uses a named cursor
    cursor = _CONN0.cursor('named_{0}'.format(randint(1, 1e9)))
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
        del rows
    cursor.close()
    del cursor

def record(sql, parameters = None):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    #
    # might not be very efficient, but leads to a bit more readable code
#    connection = factory.connection()
    cursor = _CONN0.cursor()
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
    del cursor
    return one

def execute(sql, parameters = None):
    # takes care of having to write boiler plate code for cursors and
    # opening database connections
    #
    # might not be very efficient, but leads to a bit more readable code
    cursor = _CONN1.cursor()
    try:
        log.debug(sql)
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
    except:
        raise
    _CONN1.commit()
    cursor.close()
    del cursor
