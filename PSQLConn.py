
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class PSQLConn(object):
    """Stores the connection to psql."""
    def __init__(self, db, user, password, host, port):
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        connection = psycopg2.connect(
                host=self.host,
                database=self.db,
                user=self.user,
                password=self.password,
                port = self.port)
        return connection