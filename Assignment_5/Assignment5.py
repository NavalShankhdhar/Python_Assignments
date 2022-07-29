import os
import sys

import csv
from dotenv import load_dotenv
sys.path.insert(0, '/home/neosoft/Desktop/Assignments_python/Python_Assignments/Assignment_1')
from assignment1 import MySQL, PostGreSQL

load_dotenv()
choice = int(input("\nEnter '1' to read data from MySQL database\nENter '2' to read data from PostgreSQL database\nYour choice: "))
if choice == 1:
    mysql_ob = MySQL(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'MySQL_Games')
    mysql_ob.cursor.execute('SELECT * FROM MySQL_Games.game_info')
    result = mysql_ob.cursor.fetchall()
elif choice == 2:
    psql_ob = PostGreSQL(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'allgames')
    psql_ob.cursor.execute('SELECT * FROM game_info')
    result = psql_ob.cursor.fetchall()
else:
    print("Please enter '1' or '2'.")

dir_name = "games_data"
file_name = "data_of_games.csv"
if not os.path.exists(os.path.join(os.getcwd(), dir_name)):
    os.makedirs(os.path.join(os.getcwd(), dir_name))
    file_loc = os.path.join(os.getcwd(), dir_name ,file_name)
    file = open(file_loc, 'w')
    csv.writer(file).writerows(result)