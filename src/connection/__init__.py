"""Helper module for setting up a Database connection to PostgreSQL"""

__version__ = '2.0.3'
__author__ = "Martijn Meijers"
__license__ = "MIT License"
# created: 26 jun 2012, MM

import logging
log = logging.getLogger(__name__)

import os
from ConfigParser import ConfigParser, NoOptionError
from psycopg2 import connect
import psycopg2
import warnings

def auth_params():
    config = os.environ.get('DBCONFIG', 'default')
    logging.debug("DBCONFIG: {0}".format(config))
    path = os.path.dirname(__file__)
    file_nm = os.path.join(path, 
        os.path.join(
            os.path.join("config","{0}.ini".format(config))))
    logging.debug("DBCONFIG from file: {0}".format(file_nm))
    
    # FIXME:
    # would it not be better to put config file in users home dir?
    #http://stackoverflow.com/questions/7567642/where-to-put-a-configuration-file-in-python
    # config= None
    # for loc in os.curdir, os.path.expanduser("~"), "/etc/myproject", os.environ.get("MYPROJECT_CONF"):
    #     try: 
    #         with open(os.path.join(loc,"myproject.conf")) as source:
    #             config.readfp( source )
    #     except IOError:
    #         pass
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
    dsn = "host={0} dbname={1} user={2} password={3} port={4} sslmode={5}"
    logging.debug("Connection for: {0}".format(
        dsn.format(
            auth['database'],
            auth['username'],
            '********', # hide password
            auth['host'],
            auth['port'],
            auth['sslmode'],)))
    return dsn.format(auth['host'],
        auth['database'],
        auth['username'],
        auth['password'],
        auth['port'],
        auth['sslmode'],)


class ConnectionFactory(object):
    def __init__(self):
        pass
    @classmethod
    def connection(cls, geo_enabled = True):
        warnings.warn("deprecated, will be removed in the future. use Connection class instead", DeprecationWarning)
        return Connection(dsn(), geo_enabled)

def connection(geo_enabled = False):
    """
    Factory method for new connections
    
    This is the preferred way of making new connections:
    
    >>> from connection import connection
    >>> conn = connection(geo_enabled = True)
    
    """
    return Connection.connection(geo_enabled)
    
class Connection(object):
    def __init__(self, dsn, geo_enabled = False):
        self._dsn = dsn
        self._conn = connect(dsn)
        self._key = 0
        if geo_enabled:
            self._register()
    
    def __del__(self):
        self.close()
    
    @classmethod
    def connection(cls, geo_enabled = True):
        return cls(dsn(), geo_enabled)
        
    def close(self):
        try:
            self._conn.close()
        except:
            pass

    def reconnect(self):
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
        log.debug("Registering Geometry Type (OID {0}) for PostGIS".format(geom_oid))
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
        log.debug(sql)
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
        log.debug(sql)
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
        log.debug(sql)
        one = cursor.fetchone()
        cursor.close()
#        self._conn.commit()
    #    CONN.commit()
        del cursor
        return one
    
    def execute(self, sql, parameters = None, isolation_level = 1):
        # takes care of having to write boiler plate code for cursors and
        # opening database connections
        #
        # might not be very efficient, but leads to a bit more readable code
        self._conn.set_isolation_level(isolation_level)
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
        self._conn.set_isolation_level(1)
        del cursor


    def copy_from(self, file_nm, table, sep="\t", null="\\N", size=8192, columns=None):
        cursor = self._conn.cursor()
        try:
            if columns:
                cursor.copy_from(file_nm, table, sep, null, size, columns=columns)
            else:
                cursor.copy_from(file_nm, table, sep, null, size)
        except:
            raise
        cursor.close()
        self._conn.commit()
        del cursor