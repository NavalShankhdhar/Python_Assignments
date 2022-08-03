import os
from logging import exception

import csv
from dotenv import load_dotenv

from ..Assignment_1.assignment1 import MySQL, PostGreSQL

load_dotenv()

choice = int(input("\nEnter '1' to read data from MySQL database\nENter '2' to read data from PostgreSQL database\nYour choice: "))
if choice == 1:
    mysql_ob = MySQL(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'MySQL_Games')
    mysql_ob.cursor.execute('SELECT * FROM MySQL_Games.game_info')
    data = mysql_ob.cursor.fetchall()
elif choice == 2:
    psql_ob = PostGreSQL(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'allgames')
    psql_ob.cursor.execute('SELECT * FROM game_info')
    data = psql_ob.cursor.fetchall()
else:
    print("Please enter '1' or '2'.")

dir_name = "games_data"
file_name = "data_of_games.csv"
try:
    # to check if directory already exists or not, if not it will be created
    if not os.path.exists(os.path.join(os.getcwd(), dir_name)):
        os.makedirs(os.path.join(os.getcwd(), dir_name))
    # to get path where data will be written to csv file
    file_loc = os.path.join(os.getcwd(), dir_name ,file_name)
    file = open(file_loc, 'w')
    csv.writer(file).writerows(data)
except Exception:
    print(exception)