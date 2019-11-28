"""Helper module for setting up a Database connection to PostgreSQL"""
import logging
# log = logging.getLogger(__name__)

import os
from ConfigParser import ConfigParser, NoOptionError
from psycopg2 import connect
import psycopg2
import warnings

__version__ = '2.0.4.dev0'
__author__ = "Martijn Meijers"
__license__ = "MIT License"
# created: 26 jun 2012, MM


def auth_params():
    config = os.environ.get('DBCONFIG', 'default')
    logging.debug("DBCONFIG: {0}".format(config))
    path = os.path.dirname(__file__)
    file_nm = os.path.join(path,
                           os.path.join(
                                        os.path.join("config", "{0}.ini"
                                                     .format(config))))
    logging.debug("DBCONFIG from file: {0}".format(file_nm))
    # FIXME:
    # would it not be better to put config file in users home dir?
    # http://stackoverflow.com/questions/7567642/where-to-put-a-configuration-file-in-python
    # config= None
    # for loc in os.curdir, os.path.expanduser("~"), "/etc/myproject", os.environ.get("MYPROJECT_CONF"):
    #     try:
    #         with open(os.path.join(loc,"myproject.conf")) as source:
    #             config.readfp( source )
    #     except IOError:
    #         pass
    configparser = ConfigParser()
    configparser.read(file_nm)
    auth = {}
    for key in ('host', 'database', 'username', 'password', 'port', 'sslmode'):
        try:
            auth[key] = configparser.get('database', key)
        except NoOptionError:
            auth[key] = ""
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
    dsn = []
    pg = {'database': 'dbname',
          'host': 'host',
          'username': 'user',
          'password': 'password',
          'port': 'port',
          'sslmode': 'sslmode'
          }
    for key in ('host', 'database', 'username', 'password', 'port', 'sslmode'):
        if auth[key] != "":
            dsn.append("{0}={1}".format(pg[key], auth[key]))
    dsn = " ".join(dsn)
    logging.debug("DSN: '{}'".format(dsn))
    return dsn


class connection(object):
    """A class that reduces the amount of boilerplate code to write
    for getting database connections, cursors and the like
    """
    def __init__(self, geo_enabled=True):
        self._dsn = dsn()
        self._conn = connect(self._dsn)
        self._key = 0
        if geo_enabled:
            self._register()

#     @classmethod
#     def connection(cls, geo_enabled=True):
#         return cls(dsn(), geo_enabled)

    def __enter__(self):
        return self

    def __exit__(self, tp, value, traceback):
        self.close()

    def close(self):
        try:
            self._conn.close()
        except:
            pass

    def reconnect(self):
        """Close the current connection and open it again"""
        self.close()
        self._conn = connect(self._dsn)

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
        logging.debug("Registering Geometry Type (OID {0}) for PostGIS".format(geom_oid))
        GEOMETRY = psycopg2.extensions.new_type((geom_oid, ), "GEOMETRY", loads)
        psycopg2.extensions.register_type(GEOMETRY)

    def recordset(self, sql, parameters=None):
        """Get a record set in one go (as list of tuples)

        >>> with connection() as db:
        ...     for item in db.recordset('select * from table'):
        ...        print item
        """
        cursor = self._conn.cursor()
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
        logging.debug(sql)
        rows = cursor.fetchall()
        cursor.close()
        del cursor
        return rows

    def irecordset(self, sql, parameters=None, size=2500):
        """Get a record set as generator, this makes it possible to iterate
        over the record set, while not loading the full set in main memory

        >>> with connection() as db:
        ...     for item in db.irecordset('select * from large_table'):
        ...        print item
        """
        self._key += 1
        self._conn.set_isolation_level(1)
        name = 'named_cursor__{1}_{0}'.format(self._key, os.getpid())
        cursor = self._conn.cursor(name)
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
        logging.debug(sql)
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

    def record(self, sql, parameters=None):
        """Get one record from the database"""
        cursor = self._conn.cursor()
        try:
            if parameters:
                cursor.execute(sql, parameters)
            else:
                cursor.execute(sql)
        except:
            print sql
            raise
        logging.debug(sql)
        one = cursor.fetchone()
        cursor.close()
        del cursor
        return one

    def execute(self, sql, parameters=None, isolation_level=1):
        """Execute one or multiple SQL statement(s)"""
        self._conn.set_isolation_level(isolation_level)
        cursor = self._conn.cursor()
        try:
            logging.debug(sql)
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

    def copy_from(self, file_nm, table, sep="\t", null="\\N",
                  size=8192, columns=None):
        """Use COPY to load data into the database
        (more efficient than individual inserts)
        """
        cursor = self._conn.cursor()
        try:
            if columns:
                cursor.copy_from(file_nm, table, sep, null, size, columns)
            else:
                cursor.copy_from(file_nm, table, sep, null, size)
        except:
            raise
        cursor.close()
        self._conn.commit()
        del cursor
