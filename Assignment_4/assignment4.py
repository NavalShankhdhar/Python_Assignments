import os
import sys
from mysql.connector import Error
from psycopg2.errors import Error as err
from dotenv import load_dotenv
sys.path.append('/home/neosoft/Documents/Python_assignment')
from databases_connection import MySQL, PostGreSQL
load_dotenv()

class ModifyDatabase:
    class MysqlConnection(MySQL):
        def __init__(self, host, user, password, database):
            super().__init__(host, user, password, database)
    
    class PostgresqlConnection(PostGreSQL):
        def __init__(self, host, user, password, database):
            super().__init__(host, user, password, database)
    #function to create table in both databases
    def create_table(self, query, conn):
        try: 
            self.connect = conn
            self.cursor = self.connect.cursor()
            self.cursor.execute(query)
            self.connect.commit()
            self.connect.close()
        except Error as e:
            print(e)
        except err as ee:
            print(ee)
    # function to insert data in both databases
    def insert_rows(self, query, values, conn):
        try:
            self.connect = conn
            self.cursor = self.connect.cursor()
            self.cursor.execute(query, tuple(values))
            self.connect.commit()
            self.connect.close()
        except Error as e:
            print(e)
        except err as ee:
            print(ee)

if __name__ == '__main__':
    mysql_ob = ModifyDatabase()
    mysql_conn = mysql_ob.MysqlConnection(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'ass4')
    psql_ob = ModifyDatabase()
    psql_conn = psql_ob.PostgresqlConnection(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'psqlass4')

    mysql_ob.create_table('CREATE TABLE IF NOT EXISTS USER_INFO ( ID INT AUTO_INCREMENT PRIMARY KEY, NAME VARCHAR(20) NOT NULL, PASSWORD VARCHAR (20) NOT NULL, EMAIL VARCHAR(20) NOT NULL)', mysql_conn.connect_database())
    mysql_ob.create_table('CREATE TABLE IF NOT EXISTS CATEGORY (ID INT AUTO_INCREMENT PRIMARY KEY, CATEGORY_NAME VARCHAR(25) NOT NULL UNIQUE)', mysql_conn.connect_database())
    mysql_ob.create_table('CREATE TABLE IF NOT EXISTS BOOKS (ID INT AUTO_INCREMENT PRIMARY KEY, CATEGORY_ID INT, BOOK_NAME VARCHAR(255) NOT NULL, AUTHOR_ID INT, CONSTRAINT category_name FOREIGN KEY(category_id) REFERENCES CATEGORY(ID), CONSTRAINT author_name FOREIGN KEY (author_id) REFERENCES USER_INFO(ID))', mysql_conn.connect_database())

    psql_ob.create_table('CREATE TABLE IF NOT EXISTS USER_INFO (ID SERIAL PRIMARY KEY, NAME VARCHAR(20) NOT NULL, PASSWORD VARCHAR(20) NOT NULL, EMAIL VARCHAR(20) NOT NULL)', psql_conn.connect_database())
    psql_ob.create_table('CREATE TABLE IF NOT EXISTS CATEGORY (ID SERIAL PRIMARY KEY, CATEGORY_NAME VARCHAR(25) NOT NULL UNIQUE)', psql_conn.connect_database())
    psql_ob.create_table('CREATE TABLE IF NOT EXISTS BOOKS (ID SERIAL PRIMARY KEY, CATEGORY_ID INT, BOOK_NAME VARCHAR(255) NOT NULL, AUTHOR_ID INT, CONSTRAINT category_name FOREIGN KEY(category_id) REFERENCES CATEGORY(ID), CONSTRAINT author_name FOREIGN KEY (author_id) REFERENCES USER_INFO(ID))', psql_conn.connect_database())

    new_author= input('Want to add author?(Y/N): ')
    if new_author.lower() == 'y':
        name = input('Enter user name: ')
        password= input('Enter password: ')
        email = input('Enter email: ')
        author = [name, password, email]
        mysql_ob.insert_rows('INSERT INTO USER_INFO (NAME, PASSWORD, EMAIL) VALUES (%s, %s, %s)', author, mysql_conn.connect_database())
        psql_ob.insert_rows('INSERT INTO USER_INFO (NAME, PASSWORD, EMAIL) VALUES (%s, %s, %s)', author, psql_conn.connect_database())

    new_category= input('Want to add category?(Y/N): ')
    if new_category.lower() == 'y':
        category= input('Enter category: ')
        category = [category]
        mysql_ob.insert_rows('INSERT INTO CATEGORY (CATEGORY_NAME) VALUES (%s)', category, mysql_conn.connect_database()) 
        psql_ob.insert_rows('INSERT INTO CATEGORY (CATEGORY_NAME) VALUES (%s)', category, psql_conn.connect_database()) 

    new_book = input('Want to add book? (Y/N): ') 
    if new_book.lower() == 'y':
        book_name = input('Enter book name: ')
        category_id = input('Enter category ld: ')
        author_id = input("Enter author Id: ")
        new_book = [category_id, book_name ,author_id]
        mysql_ob.insert_rows('INSERT INTO BOOKS (CATEGORY_ID, BOOK_NAME, AUTHOR_ID) VALUES (%s, %s, %s)', new_book,  mysql_conn.connect_database())
        psql_ob.insert_rows('INSERT INTO BOOKS (CATEGORY_ID, BOOK_NAME, AUTHOR_ID) VALUES (%s, %s, %s)', new_book, psql_conn.connect_database())