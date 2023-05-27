from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

class postgresEngine:
    def __init__(self):
        self.pg_conn_string = environ["POSTGRESQL_CS"]

    def create(self):
        while True:
            try:
                psql_engine = create_engine(self.pg_conn_string)
                break
            except OperationalError:
                sleep(0.1)
        print('Connection to PostgresSQL successful.')
        return psql_engine