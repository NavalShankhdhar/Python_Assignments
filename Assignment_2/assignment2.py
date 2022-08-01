import os
import csv
import logging

from fpdf import FPDF
from dotenv import load_dotenv
from ..Assignment_1.assignment1 import MySQL, PostGreSQL

load_dotenv()

class PDF(FPDF):
    #function to add header, it is automatically called when add_page() is callled
    def header(self):
        self.set_font("helvetica","B", 20)
        self.cell(self.epw, 0, "Games", 0, align='C')
        self.ln(20)

    # function to add footer, it is automatically called when close() is called
    def footer(self):
        self.ln(10)
        self.set_font("helvetica",'I', 12)
        self.cell(self.epw, 0, "Page" + str(self.page_no()) + "/{nb}", 0, align='C')

    # function to make the headings of table bold
    def render_table_header(self):
        self.set_font(style = "B")  
        for col_name in table_col:
            self.cell(self.col_width, self.line_height, col_name, border=1)
        self.ln(self.line_height)
        self.set_font(style = "")

    # function to create table and pdf
    def create_pdf(self, data, table_col):
        self.table_col = table_col
        self.line_height = self.font_size * 2.5
        self.col_width = self.epw / len(table_col)
        self.render_table_header()
        for row in data:
            if self.will_page_break(self.line_height):
                self.render_table_header()
            for cell in row:
                if str(cell) == 'None':
                    cell = ''
                self.multi_cell(self.col_width, self.line_height, str(cell), border=1, new_x="RIGHT", new_y="TOP", max_line_height=self.font_size)
            self.ln(self.line_height)
        self.output('Output.pdf')


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
        table_col = ("Game name", "Game Type", "Game size", "Mode")
        pdf.create_pdf(result, table_col)
    except Exception as e:
        logging.error(e)