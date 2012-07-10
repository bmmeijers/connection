import os
import logging
from ConfigParser import ConfigParser, NoOptionError
from psycopg2 import connect

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
    logging.debug("Connecting to: {}".format(
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
        self.dsn = dsn()
    
    def connection(self):
        return connect(self.dsn)
