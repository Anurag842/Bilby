

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import locale
import snowflake.connector
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Indonesian date handling
locale.setlocale(locale.LC_TIME, 'id_ID.utf8')

def fetch_article_data(url):
  
    try:
        response = requests.get(url)
        response.raise_for_status() 
        soup = BeautifulSoup(response.content, 'html.parser')

        title = soup.find('h1', class_='f50 black2 f400 crimson').text.strip()
        
        body = ' '.join([p.text for p in soup.find('div', class_='fl pt20 pos_rel').find_all('p')])

        author = soup.find('div', class_='f20 credit mt10').text.strip()
        author_extracted = (author[(author.find(':')+1):(author.find('Editor:'))].strip()).replace("\n", "").strip()
        
        #Stripping and converting the date
        date = soup.find('div', class_='grey bdr3 pb10 pt10').text.strip()
        date_str = date.replace("Tayang: ", "").split(" WIB")[0]
        date_format = "%A, %d %B %Y %H:%M" 
        date_obj = datetime.strptime(date_str, date_format)

        return title, body, author_extracted, date_obj

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching the URL: {e}")
        raise
    except Exception as e:
        logging.error(f"Error parsing the article data: {e}")
        raise

def snowflake_dump(title, body, author, date):
   
    try:
        #snowflake connection details
        conn = snowflake.connector.connect(
            user='anuragdummyproton3',
            password='Anuragdummyproton3',
            account='lk35165.ap-south-1',
            warehouse='COMPUTE_WH',
            database='BILBY',
            schema='STAGING'
        )


        cur = conn.cursor()

      
        create_table_query = """
        CREATE OR REPLACE TABLE articles (
            title STRING,
            body STRING,
            author STRING,
            date TIMESTAMP
        );
        """
        cur.execute(create_table_query)

        insert_query = """
        INSERT INTO articles (title, body, author, date)
        VALUES (%s, %s, %s, %s)
        """
        cur.execute(insert_query, (title, body, author, date))

        conn.commit()
        logging.info("Article data inserted into Snowflake successfully!")

    except snowflake.connector.errors.DatabaseError as e:
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error inserting data into Snowflake: {e}")
        raise
    finally:
       
        if cur:
            cur.close()
        if conn:
            conn.close()

def main():

    url = 'https://www.tribunnews.com/new-economy/2024/08/07/lewat-teknologi-dan-edukasi-gopay-mendukung-pemberantasan-judi-online-di-indonesia'
    #Fetch the data
    title, body, author, date = fetch_article_data(url)
    #Insert the data
    snowflake_dump(title, body, author, date)


main()




