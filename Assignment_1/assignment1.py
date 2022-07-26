import csv
import os
import sys

from mysql.connector import Error
from psycopg2.errors import Error as err
from dotenv import load_dotenv
sys.path.append('/home/neosoft/Downloads/Python_Assignments')
from databases_connection import MySQL, PostGreSQL
load_dotenv()

# class to parse csv file
class CsvParse:
    def __init__(self, file_name, passed_delimiter):
        self.csv_file_name = file_name
        self.passed_delimiter = passed_delimiter
    #function to read csv file
    def read_csv(self):
        try:
            with open(self.csv_file_name, 'r') as file:
                csv_reader = list(csv.reader(file, delimiter = self.passed_delimiter))
            return csv_reader
        except Error as e:
            print("Error while reading csv file ", e)
    #function to add new row to csv file
    def add_row(self, new_row):
        try:
            with open(self.csv_file_name, 'a') as file:
                csv.writer(file, delimiter = self.passed_delimiter).writerow(new_row)
        except Error as e:
            print("Error while writing to csv file ", e)

class ModifyDatabase:
    class MysqlConnection(MySQL):
        def __init__(self, host, user, password, database):
            super().__init__(host, user, password, database)
    
    class PostgresqlConnection(PostGreSQL):
        def __init__(self, host, user, password, database):
            super().__init__(host, user, password, database)
    #function to create table in MySQL and PostgreSQL databases
    def create_table(self, csv_file1, csv_file2, query, insert, conn):
        self.sql = insert
        try:
            self.connect = conn
            self.cursor = self.connect.cursor()
            self.cursor.execute(query)
            self.connect.commit()
            self.insert_data(csv_file1, csv_file2)
        except Error:
            pass
        except err:
            pass
    #function to insert existing data to newly created table
    def insert_data(self, csv_file1, csv_file2):
        try:
            for row in csv_file1:
                self.cursor.execute(self.sql, tuple(row))
            self.connect.commit()
            for row in csv_file2:
                self.cursor.execute(self.sql, tuple(row))
            self.connect.commit()
            self.cursor.close()
        except Error as e:
            print("Error while inserting rows to MySQL database ", e)
        except err as e:
            print("Error while inserting rows to PostgreSQL database ", e)
    #function to insert new row to MySQL and PostgreSQL databases
    def new_insert(self, new, conn):
        try:
            self.connect = conn
            self.cursor = self.connect.cursor()
            self.cursor.execute(self.sql, tuple(new))
            self.connect.commit()
            self.cursor.close()
        except Error as e:
            print("Error while inserting new row to MYSQL database ", e)
        except err as e:
            print("Error while inserting new row to PostgreSQL ", e)

if __name__ == "__main__":
    #objects of CSV parse class
    csv_ob1 = CsvParse("games.csv", ",")
    csv_ob2 = CsvParse("games2.csv", ";")

    mysql_query = """
                    CREATE TABLE games_info(
                        ID INT AUTO_INCREMENT PRIMARY KEY,
                        Game_name VARCHAR(50) NOT NULL,
                        Game_type VARCHAR(25) NOT NULL,
                        Game_size FLOAT,
                        Game_mode VARCHAR(20)
                )"""
    psql_query = """
                    CREATE TABLE games_info(
                        ID SERIAL PRIMARY KEY,
                        Game_name VARCHAR(50) NOT NULL,
                        Game_type VARCHAR(25) NOT NULL,
                        Game_size FLOAT,
                        Game_mode VARCHAR(20)
                )"""
    # mysql connection, table creation and insertion
    mysql_ob = ModifyDatabase()
    mysql_conn = mysql_ob.MysqlConnection(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'MySQL_Games')
    insert_into = """
                    INSERT INTO MySQL_Games.games_info(
                        Game_name,
                        Game_type,
                        Game_size,
                        Game_mode
                        )
                    VALUES (%s,%s,NULLIF(%s,''),NULLIF(%s,''))
                """
    mysql_ob.create_table(csv_ob1.read_csv(), csv_ob2.read_csv(), mysql_query, insert_into, mysql_conn.connect_database())

    # mysql connection, table creation and insertion
    psql_ob = ModifyDatabase()
    psql_conn = psql_ob.PostgresqlConnection(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'allgames')
    insert_into_psql = """
                        INSERT INTO games_info(
                            Game_name,
                            Game_type,
                            Game_size,
                            Game_mode
                            ) 
                        VALUES (%s,%s,NULLIF(%s,0.0),NULLIF(%s,''))
                    """
    read_csv1 = csv_ob1.read_csv()
    readcsv2 = csv_ob2.read_csv()
    # to check the Game_size column fields that are empty and are float type and return 0 for those fields
    for data in read_csv1:
        if data[2] == '':
            data[2] = 0
    for data in readcsv2:
        if data[2] == '':
            data[2] = 0
    psql_ob.create_table(read_csv1, readcsv2, psql_query, insert_into_psql, psql_conn.connect_database())

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
        mysql_ob.new_insert(new_csv_row, mysql_conn.connect_database())
        new_csv_row[2] = 0
        psql_ob.new_insert(new_csv_row, psql_conn.connect_database())
