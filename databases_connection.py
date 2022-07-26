import mysql.connector as mysql
from mysql.connector import Error
import psycopg2
from psycopg2.errors import Error as err

class ConnectDatabase:
    #initialize connection details
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()

class MySQL(ConnectDatabase):
    #function to connect to MySQL server
    def connect_database(self):
        try:
            mysql_conn = mysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                database = self.database
            )
            return mysql_conn
        except Error as e:
            print("Error while connecting to MySQL", e)

class PostGreSQL(ConnectDatabase):
    #function to connect to postgresql server
    def connect_database(self):
        try:
            psql_conn = psycopg2.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                database = self.database,
                port = '5432'
            )
            return psql_conn
        except err as e:
            print("Error while connecting to PostgreSQL", e)