# created: 26 jun 2012, MM
"""Helper module for setting up a Database connection to PostgreSQL"""

__version__ = "1.0.0"
__author__ = "Martijn Meijers"
__license__ = "MIT License"

import logging
log = logging.getLogger(__name__)

import os
from ConfigParser import ConfigParser, NoOptionError
from psycopg2 import connect
import psycopg2

def auth_params():
    config = os.environ.get('DBCONFIG', 'localhost')
    logging.debug("DBCONFIG: {}".format(config))
    path = os.path.dirname(__file__)
    file_nm = os.path.join(path, 
        os.path.join(
            os.pardir,
            os.path.join("config","{}.ini".format(config))))
    configparser = ConfigParser()
    configparser.read(file_nm)
    try:
        sslmode = configparser.get('database', 'sslmode')
    except NoOptionError:
        sslmode = "prefer"
    auth = {
        "database": configparser.get('database', 'database'),
        "username": configparser.get('database', 'username'),
        "password": configparser.get('database', 'password'),
        "host": configparser.get('database', 'host'),
        "port": configparser.get('database', 'port'),
        "sslmode": sslmode,}
    return auth

def dsn():
    # PostgreSQL has the following parameters for connections:
    # dbname - the database name (only in dsn string)
    # database - the database name (only as keyword argument)
    # user - user name used to authenticate
    # password - password used to authenticate
    # host - database host address (defaults to UNIX socket if not provided)
    # port - connection port number (defaults to 5432 if not provided)
    # sslmode - SSL TCP/IP negotiation mode
    auth = auth_params()
    dsn = "dbname={} user={} password={} host={} port={} sslmode={}"
    logging.debug("Connection for: {}".format(
        dsn.format(
            auth['database'],
            auth['username'],
            '********', # hide password
            auth['host'],
            auth['port'],
            auth['sslmode'],)))
    return dsn.format(
        auth['database'],
        auth['username'],
        auth['password'],
        auth['host'],
        auth['port'],
        auth['sslmode'],)


class ConnectionFactory(object):
    def __init__(self):
        pass
    @classmethod
    def connection(cls, geo_enabled = True):
        return Connection(dsn(), geo_enabled)

class Connection(object):
    def __init__(self, dsn, geo_enabled = False):
        self._dsn = dsn
        self._conn = connect(dsn)
        self._key = 0
        if geo_enabled:
            self._register()
    
    def close(self):
        try:
            self._conn.close()
        except:
            pass

    def reopen(self):
        self.close()
        self._conn = connect(self._dsn)
    
#    def __del__(self):
#        self.close()

    def _register(self):
        """Find the correct OID and register the input/output function
         as psycopg2 extension type for automatic type conversion to happen.
        
        .. note::
            Should be called *only* once
        """
        from simplegeom.wkb import loads
        cursor = self._conn.cursor()
        cursor.execute("SELECT NULL::geometry")
        geom_oid = cursor.description[0][1]
        cursor.close()
        log.debug("Registering Geometry Type (OID {}) for PostGIS".format(geom_oid))
        GEOMETRY = psycopg2.extensions.new_type((geom_oid, ), "GEOMETRY", loads)
        psycopg2.extensions.register_type(GEOMETRY)

    def recordset(self, sql, parameters = None):
        # takes care of having to write boiler plate code for cursors and
        # opening database connections
        #
        # might not be very efficient, but leads to a bit more readable code
#        self._conn.set_isolation_level(1)
        cursor = self._conn.cursor()
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
#        self._conn.commit()
        del cursor
        return rows
    
    def irecordset(self, sql, parameters = None, size = 2500):
        # takes care of having to write boiler plate code for cursors and
        # opening database connections
        #
        # might not be very efficient, but leads to a bit more readable code
        #
        # NOTE: do not nest irecordset calls inside loops, otherwise
        # things *will* go wrong. If needed, use multiple connection objects
        #
        self._key += 1
        self._conn.set_isolation_level(1)
        name = 'named_cursor__{1}_{0}'.format(self._key, os.getpid())
        cursor = self._conn.cursor(name)
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
        self._conn.commit()
        del rows
        del cursor
    
    def record(self, sql, parameters = None):
        # takes care of having to write boiler plate code for cursors and
        # opening database connections
        #
        # might not be very efficient, but leads to a bit more readable code
#        self._conn.set_isolation_level(1)
        cursor = self._conn.cursor()
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
#        self._conn.commit()
    #    CONN.commit()
        del cursor
        return one
    
    def execute(self, sql, parameters = None):
        # takes care of having to write boiler plate code for cursors and
        # opening database connections
        #
        # might not be very efficient, but leads to a bit more readable code
#        self._conn.set_isolation_level(1)
        cursor = self._conn.cursor()
        try:
            log.debug(sql)
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)
        except:
            raise
        cursor.close()
        self._conn.commit()
        del cursor
