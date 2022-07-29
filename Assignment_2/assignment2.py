import sys
import os
import csv
import logging

from fpdf import FPDF
from dotenv import load_dotenv
sys.path.insert(0, '/home/neosoft/Desktop/Assignments_python/Python_Assignments/Assignment_1')
from assignment1 import MySQL, PostGreSQL

load_dotenv()

class PDF(FPDF):
    def header(self):
        self.set_font("helvetica","B", 20)
        self.cell(pdf.epw, 0, "Games", 0, align='C')
        self.ln(20)

    def footer(self):
        self.ln(10)
        self.set_font("helvetica",'I', 12)
        self.cell(pdf.epw, 0, "Page" + str(self.page_no()) + "/{nb}", 0, align='C')

if __name__ == "__main__":
    choice = int(input("\nEnter 1 to create pdf from MySql database\nEnter 2 to create pdf from Postgresql database\nEnter 3 to create pdf from csv file\n\nYour choice: "))
    if choice == 1:
        mysql_ob = MySQL(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'MySQL_Games')
        mysql_ob.cursor.execute('SELECT Game_name, Game_type, Game_size, Game_mode FROM MySQL_Games.game_info')
        result = mysql_ob.cursor.fetchall()

    elif choice == 2:
        psql_ob = PostGreSQL(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'allgames')
        psql_ob.cursor.execute('SELECT Game_name, Game_type, Game_size, Game_mode FROM game_info')
        result = psql_ob.cursor.fetchall()

    elif choice == 3:
        with open ("games.csv",'r') as file:
            result = tuple(csv.reader(file))
    else:
        print("Please enter 1, 2 or 3.")
        exit()
    try:
        pdf = PDF()
        pdf.add_page()
        pdf.set_font("Times", size = 15)
        line_height = pdf.font_size * 2.5
        col_width = pdf.epw / 4
    except Exception as e:
        logging.error(e)

    table_col = ("Game name", "Game Type", "Game size", "Mode")
    def render_table_header():
        pdf.set_font(style="B")  
        for col_name in table_col:
            pdf.cell(col_width, line_height, col_name, border=1)
        pdf.ln(line_height)
        pdf.set_font(style="")
    render_table_header()
    try:
        for row in result:
            if pdf.will_page_break(line_height):
                render_table_header()
            for data in row:
                if str(data) == 'None':
                    data = ''
                pdf.multi_cell(col_width, line_height, str(data), border=1, new_x="RIGHT", new_y="TOP", max_line_height=pdf.font_size)
            pdf.ln(line_height)
        pdf.output('Output.pdf')
    except Exception as e:
        logging.error(e)