from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

class mysqlEngine:
    def __init__(self):
        self.ms_conn_string = environ["MYSQL_CS"]

    def create(self):
        while True:
            try:
                msql_engine = create_engine(self.ms_conn_string, pool_pre_ping=True, pool_size=10)
                break
            except OperationalError:
                sleep(0.1)
        print('Connection to MYSQL successful.')
        return msql_engine