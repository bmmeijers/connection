import os
import logging
from ConfigParser import ConfigParser
from psycopg2 import connect

def _dsn():
    # PostgreSQL has the following parameters for connections:
    # dbname - the database name (only in dsn string)
    # database - the database name (only as keyword argument)
    # user - user name used to authenticate
    # password - password used to authenticate
    # host - database host address (defaults to UNIX socket if not provided)
    # port - connection port number (defaults to 5432 if not provided)
    # sslmode - SSL TCP/IP negotiation mode
    configparser = ConfigParser()
    config = os.environ.get('DBCONFIG', 'localhost')
    logging.debug("DBCONFIG: {}".format(config))
    path = os.path.dirname(__file__)
    file_nm = os.path.join(path, 
        os.path.join(
            os.pardir,
            os.path.join("config","{}.ini".format(config))))

    configparser.read(file_nm)
    try:
        sslmode = configparser.get('database', 'sslmode')
    except ConfigParser.NoOptionError:
        sslmode = "prefer"
    dsn = "dbname={} user={} password={} host={} port={} sslmode={}"
    logging.debug("Connecting to: {}".format(
        dsn.format(
            configparser.get('database', 'database'),
            configparser.get('database', 'username'),
            '********', # hide password
            configparser.get('database', 'host'),
            configparser.get('database', 'port'),
            sslmode,)))
    return dsn.format(
        configparser.get('database', 'database'),
        configparser.get('database', 'username'),
        configparser.get('database', 'password'),
        configparser.get('database', 'host'),
        configparser.get('database', 'port'),
        sslmode,)


class ConnectionFactory(object):
    def __init__(self):
        self.dsn = _dsn()
    
    def connection(self):
        return connect(self.dsn)
