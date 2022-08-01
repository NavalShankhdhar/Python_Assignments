import csv
import os

import mysql.connector
from mysql.connector import Error
import psycopg2
from psycopg2 import Error as error
from dotenv import load_dotenv

load_dotenv()
# class to parse csv file
class CsvParse:
    def __init__(self, file_name, delimiter):
        self.csv_file_name = file_name
        self.delimiter = delimiter

    #function to read csv file
    def read_csv(self):
        with open(self.csv_file_name, 'r') as file:
            csv_reader = list(csv.reader(file, delimiter = self.delimiter))
        return csv_reader

    #function to add new row to csv file
    def add_row(self, new_row):
        with open(self.csv_file_name, 'a') as file:
            csv.writer(file, delimiter = self.delimiter).writerow(new_row)

class ConnectDatabase:
    #initialize connection details
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()

    # function to check if table is empty or not
    def is_empty(self):
        self.cursor.execute("SELECT COUNT(*) AS rowcount FROM MySQL_Games.games_info")
        if self.cursor.fetchone()[0]:
            return False
        else:
            return True

class MySQL(ConnectDatabase):
    #function to connect to MySQL server
    def connect_database(self):
        mysql_conn = mysql.connector.connect(
            host = self.host,
            user = self.user,
            password = self.password,
            database = self.database
        )
        return mysql_conn

    #function to create table in MySQL database
    def create_table(self):
        self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS games_info(
                        ID INT AUTO_INCREMENT PRIMARY KEY,
                        Game_name VARCHAR(50) NOT NULL,
                        Game_type VARCHAR(25) NOT NULL,
                        Game_size FLOAT,
                        Game_mode VARCHAR(20)
                )""")
        self.connect.commit()

    # function to insert existing data from csv to MySQL database
    def insert_data(self, csv_file1, csv_file2):
        self.mysql_query = """
                    INSERT INTO MySQL_Games.games_info(
                        Game_name,
                        Game_type,
                        Game_size,
                        Game_mode
                        )
                    VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''))
                """
        for row in csv_file1:
            self.cursor.execute(self.mysql_query, tuple(row))
        self.connect.commit()
        for row in csv_file2:
            self.cursor.execute(self.mysql_query, tuple(row))
        self.connect.commit()
        self.cursor.close()

    #function to insert new data to database
    def new_insert(self, new):
        self.cursor.execute("""
                    INSERT INTO MySQL_Games.games_info(
                        Game_name,
                        Game_type,
                        Game_size,
                        Game_mode
                        )
                    VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''))
                """, tuple(new))
        self.connect.commit()
        self.cursor.close()

class PostGreSQL(ConnectDatabase):
    #function to connect to postgresql server
    def connect_database(self):
        psql_conn = psycopg2.connect(
            host = self.host,
            user = self.user,
            password = self.password,
            database = self.database,
            port = '5432'
        )
        return psql_conn

    #function to create table in Postgresql database
    def create_table(self):
        self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS games_info(
                        ID SERIAL PRIMARY KEY,
                        Game_name VARCHAR(50) NOT NULL,
                        Game_type VARCHAR(25) NOT NULL,
                        Game_size FLOAT,
                        Game_mode VARCHAR(20)
                )""")
        self.connect.commit()

    # function to insert existing data from csv to Postgresql database
    def insert_data(self, csv_file1, csv_file2):
        self.psql_query = """
                        INSERT INTO games_info(
                            Game_name,
                            Game_type,
                            Game_size,
                            Game_mode
                            ) 
                        VALUES (%s,%s,NULLIF(%s,0.0),NULLIF(%s,''))
                    """
        for row in csv_file1:
            self.cursor.execute(self.psql_query, tuple(row))
        self.connect.commit()
        for row in csv_file2:
            self.cursor.execute(self.psql_query, tuple(row))
        self.connect.commit()
        self.cursor.close()
    
    # function to insert new data to database
    def new_insert(self, new):
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()
        self.cursor.execute( """
                        INSERT INTO games_info(
                            Game_name,
                            Game_type,
                            Game_size,
                            Game_mode
                            ) 
                        VALUES (%s,%s,NULLIF(%s,0.0),NULLIF(%s,''))
                    """, tuple(new))
        self.connect.commit()
        self.cursor.close()

if __name__ == "__main__":
    #objects of CSV parse class
    csv_ob1 = CsvParse("games.csv", ",")
    csv_ob2 = CsvParse("games2.csv", ";")

    try:
        # mysql connectionand table creation
        mysql_ob = MySQL(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'MySQL_Games')
        psql_ob = PostGreSQL(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'allgames')
        mysql_ob.create_table()
        psql_ob.create_table()
    except Error as e:
        print("Cannot connect to MySQL database or unable to create MySQL table ", e)
    except error as er:
        print("Cannot connect to Postgresql database or unable to create Postgresql database ", er)

    # to check if table is empty
    if mysql_ob.is_empty():
        try:
            mysql_ob.insert_data(csv_ob1.read_csv(), csv_ob2.read_csv())
            read_csv1 = csv_ob1.read_csv()
            readcsv2 = csv_ob2.read_csv()
            # to check the (Game_size) column cells that are empty and are float type, so return 0 for those cells
            for data in read_csv1:
                if data[2] == '':
                    data[2] = 0
            for data in readcsv2:
                if data[2] == '':
                    data[2] = 0
            psql_ob.insert_data(read_csv1, readcsv2)
        except Error as e:
            print("Cannot insert row to MySQL database ", e)
        except error as er:
            print("Cannot insert row to Postgresql database ", er)

    #user input to add new row to csv, mysql and postgresql databases
    new_row = input("Do you want to add new row to csv file?(Y/N): ")
    if new_row.lower() == 'y':
        name = input("\nEnter the name of game: ")
        type = input("What type of game it is(action, racing, shooting...) ?: ")
        size = input("Enter the size of game in GB(Optional): ")
        if size != '':
            size = float(size)
        mode = input("Is it Singleplayer or Multiplayer game(Optional): " )
        deli = input("\nType '1' to input data in 1st CSV file or '2' to input data in 2nd CSV file: ")
        new_csv_row = [name, type, size, mode]
        if deli == '1':
            csv_ob1.add_row(new_csv_row)
        elif deli == '2':
            csv_ob2.add_row(new_csv_row)
        else:
            print("Please enter '1' or '2'")
        mysql_ob.new_insert(new_csv_row)
        new_csv_row[2] = 0
        psql_ob.new_insert(new_csv_row)