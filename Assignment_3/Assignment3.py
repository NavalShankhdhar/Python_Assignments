import os
import logging

import requests
import urllib.request
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from ..Assignment_1.assignment1 import MySQL, PostGreSQL

load_dotenv()

class MysqlConnection(MySQL):
    # function to create table in MySQL database
    def create_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS product_info(
                                    product_title VARCHAR(255),
                                    product_price FLOAT,
                                    product_rating VARCHAR(50),
                                    review_count VARCHAR(50),
                                    availability VARCHAR(50),
                                    image_link VARCHAR(255),
                                    image_binary LONGBLOB)
                            """)
        self.connect.commit()
    # function to insert data in MySQL table
    def insert_row(self, data):
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()
        self.cursor.execute("""INSERT INTO product_info(product_title, product_price, product_rating, review_count, availability, image_link, image_binary) 
                                VALUES(%s,%s,%s,%s,%s,%s,%s)""", data)
        self.connect.commit()
        self.cursor.close()


class PostgresqlConnection(PostGreSQL):
    # function to create table in Postgresql database
    def create_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS product_info(
                                    product_title VARCHAR(255),
                                    product_price FLOAT,
                                    product_rating VARCHAR(50),
                                    review_count VARCHAR(50),
                                    availability VARCHAR(50),
                                    image_link VARCHAR(255),
                                    image_binary bytea)
                            """)
        self.connect.commit()
    # function to insert data in Postgresql table
    def insert_row(self, data):
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()
        self.cursor.execute("""INSERT INTO product_info(product_title, product_price, product_rating, review_count, availability, image_link, image_binary)
                                VALUES(%s,%s,%s,%s,%s,%s,%s)""", data)
        self.connect.commit()
        self.cursor.close()


class Scrape:
    def __init__(self, soup):
        self.soup = soup
    # Function to extract Product Title
    def get_title(self):
        title = self.soup.find("span", attrs={"id":'productTitle'})
        title_value = title.string
        title_string = title_value.strip()
        return title_string

    # Function to extract Product Price
    def get_price(self):
        price = self.soup.find("span", attrs={'class':'a-price-whole'})
        price = price.next_element.replace(',', '')
        return price

    # Function to extract Product Rating
    def get_rating(self):
        try:
            rating = self.soup.find("i", attrs={'class':'a-icon a-icon-star a-star-4-5'}).string.strip()

        except AttributeError:
            try:
                rating = self.soup.find("span", attrs={'class':'a-icon-alt'}).string.strip()
            except:
                    rating = ""	
        return rating

    # Function to extract Number of User Reviews
    def get_review_count(self):
        review_count = self.soup.find("span", attrs={'id':'acrCustomerReviewText'}).string.strip()
        return review_count

    # Function to extract Availability Status
    def get_availability(self):
        try:
            available = self.soup.find("div", attrs={'id':'availability'})
            available = available.find("span").string.strip()
        except AttributeError:
            available = "Not Available"	
        return available

    #Function to extract image link of product
    def get_images(self):
        try:
            images = self.soup.find_all("img", attrs={'class': 'a-dynamic-image a-stretch-horizontal'})
            src = images[0]['src']
        except IndexError:
            src =""
        return src


if __name__ == '__main__':
    try:
        mysql_ob = MysqlConnection(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'productdb')
        psql_ob = PostgresqlConnection(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'productdb')

        mysql_ob.create_table()
        psql_ob.create_table()
    except Exception as e:
        logging.error("Unable to connect to database or create table:", e)
    try:
        HEADERS = ({'User-Agent':'Chrome/103.0.5060.134', 'Accept-Language': 'en-US'})
        # The webpage URL
        URL = "https://www.amazon.in/s?k=mobiles&ref=nb_sb_noss"
        # HTTP Request
        webpage = requests.get(URL, headers=HEADERS)
        # Soup Object containing all data
        soup = BeautifulSoup(webpage.content, "lxml")
        # Fetch links as List of Tag Objects
        links = soup.find_all("a", attrs={'class':'a-link-normal s-no-outline'}, limit=20)
        # Store the links
        links_list = []
        image_count = 1
        # Loop for extracting links from Tag Objects
        for link in links:
            links_list.append(link.get('href'))

        # Loop for extracting product details from each link 
        for link in links_list:
            new_webpage = requests.get("https://www.amazon.in" + link, headers=HEADERS)
            new_soup = BeautifulSoup(new_webpage.content, "lxml")
            scrape_ob = Scrape(new_soup)

            data_list = []
            data_list.extend((scrape_ob.get_title(), scrape_ob.get_price(), scrape_ob.get_rating(), 
                scrape_ob.get_review_count(), scrape_ob.get_availability(), scrape_ob.get_images()))
            image_name = "image" + str(image_count) + ".jpg"
            print(data_list)
            image_count += 1
            # if images are available
            if data_list[5] != '':
                # store the images in a folder
                urllib.request.urlretrieve(data_list[5],os.path.join(os.getcwd(), 'Assignment_3','Images', image_name))
                # convert the image to binary format
                data = open(os.path.join(os.getcwd(), 'Assignment_3','Images', image_name), 'rb').read()
                data_list.append(data)
            # if image link is not available
            else:
                data_list.append("")
            mysql_ob.insert_row(data_list)
            psql_ob.insert_row(data_list)
    except Exception as e:
        logging.error("Unable to extract or store data from website")