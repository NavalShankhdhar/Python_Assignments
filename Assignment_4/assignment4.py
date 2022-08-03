import os
from logging import exception

from dotenv import load_dotenv

from ..Assignment_1.assignment1 import MySQL, PostGreSQL

load_dotenv()

class MysqlConnection(MySQL):

    def create_mtable1(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS USER_INFO ( ID INT AUTO_INCREMENT PRIMARY KEY, NAME VARCHAR(20) NOT NULL, PASSWORD VARCHAR (20) NOT NULL, EMAIL VARCHAR(20) NOT NULL)')
        self.connect.commit()

    def create_mtable2(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS CATEGORY (ID INT AUTO_INCREMENT PRIMARY KEY, CATEGORY_NAME VARCHAR(25) NOT NULL UNIQUE)')
        self.connect.commit()

    def create_mtable3(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS BOOKS (
                                       ID INT AUTO_INCREMENT PRIMARY KEY,
                                       CATEGORY_ID INT,
                                       BOOK_NAME VARCHAR(255) NOT NULL,
                                       AUTHOR_ID INT,
                                       CONSTRAINT category_name FOREIGN KEY(category_id) REFERENCES CATEGORY(ID),
                                       CONSTRAINT author_name FOREIGN KEY (author_id) REFERENCES USER_INFO(ID)
                                       )""")
        self.connect.commit()

    def insert_rows1(self, values):
        self.cursor.execute('INSERT INTO USER_INFO (NAME, PASSWORD, EMAIL) VALUES (%s, %s, %s)', tuple(values))
        self.connect.commit()
        self.connect.close()

    def insert_rows2(self, values):
        self.cursor.execute('INSERT INTO CATEGORY (CATEGORY_NAME) VALUES (%s)', tuple(values))
        self.connect.commit()
        self.connect.close()

    def insert_rows3(self, values):
        self.cursor.execute('INSERT INTO BOOKS (CATEGORY_ID, BOOK_NAME, AUTHOR_ID) VALUES (%s, %s, %s)', tuple(values))
        self.connect.commit()
        self.connect.close()


class PostgresqlConnection(PostGreSQL):

    def create_ptable1(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS USER_INFO (ID SERIAL PRIMARY KEY, NAME VARCHAR(20) NOT NULL, PASSWORD VARCHAR(20) NOT NULL, EMAIL VARCHAR(20) NOT NULL)')
        self.connect.commit()

    def create_ptable2(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS CATEGORY (ID SERIAL PRIMARY KEY, CATEGORY_NAME VARCHAR(25) NOT NULL UNIQUE)')
        self.connect.commit()

    def create_ptable3(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS BOOKS (
                                       ID SERIAL PRIMARY KEY,
                                       CATEGORY_ID INT,
                                       BOOK_NAME VARCHAR(255) NOT NULL,
                                       AUTHOR_ID INT,
                                       CONSTRAINT category_name FOREIGN KEY(category_id) REFERENCES CATEGORY(ID),
                                       CONSTRAINT author_name FOREIGN KEY (author_id) REFERENCES USER_INFO(ID)
                                       )""")
        self.connect.commit()

    def insert_rows1(self, values):
        self.cursor.execute('INSERT INTO USER_INFO (NAME, PASSWORD, EMAIL) VALUES (%s, %s, %s)', tuple(values))
        self.connect.commit()
        self.connect.close()

    def insert_rows2(self, values):
        self.cursor.execute('INSERT INTO CATEGORY (CATEGORY_NAME) VALUES (%s)', tuple(values))
        self.connect.commit()
        self.connect.close()
        
    def insert_rows3(self, values):
        self.cursor.execute('INSERT INTO BOOKS (CATEGORY_ID, BOOK_NAME, AUTHOR_ID) VALUES (%s, %s, %s)', tuple(values))
        self.connect.commit()
        self.connect.close()

if __name__ == '__main__':
    try:
        mysql_ob = MysqlConnection(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'books')
        psql_ob = PostgresqlConnection(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'psqlbooks')
        
        mysql_ob.create_mtable1()
        mysql_ob.create_mtable2()
        mysql_ob.create_mtable3()

        psql_ob.create_ptable1()
        psql_ob.create_ptable2()
        psql_ob.create_ptable3()

        new_author= input('Want to add author?(Y/N): ')
        if new_author.lower() == 'y':
            name = input('Enter user name: ')
            password= input('Enter password: ')
            email = input('Enter email: ')
            author = [name, password, email]
            mysql_ob.insert_rows1(author)
            psql_ob.insert_rows1(author)

        new_category= input('Want to add category?(Y/N): ')
        if new_category.lower() == 'y':
            category= input('Enter category: ')
            category = [category]
            mysql_ob.insert_rows2(category) 
            psql_ob.insert_rows2(category) 

        new_book = input('Want to add book? (Y/N): ') 
        if new_book.lower() == 'y':
            book_name = input('Enter book name: ')
            category_id = input('Enter category ld: ')
            author_id = input("Enter author Id: ")
            new_book = [category_id, book_name ,author_id]
            mysql_ob.insert_rows3(new_book)
            psql_ob.insert_rows3(new_book)
    except Exception:
        print(exception)
