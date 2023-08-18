import requests
import json
import math
import time
import concurrent.futures
import duckdb
import pandas as pd

from tqdm import tqdm
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import NoSuchElementException

class Scraper:

    def __init__(self) -> None:
        self.conn = None
        self.parser = 'html.parser'
        self.max_workers = 20

    def get_band_page_links(self, headers, letter):
        query_interval = 0
        page_number = 1
        base_url = f'https://www.metal-archives.com/browse/ajax-letter/l/{letter}/json/1?sEcho={page_number}&iDisplayStart={query_interval}&iSortCol_0=0&sSortDir_0=asc'
        response = requests.get(base_url, headers=headers)

        band_page_links = []

        if response.status_code == 200:

            json_data = json.loads(response.text)

            aaData = json_data.get('aaData', [])
            for band_info in aaData:
                band_href = band_info[0]
                band_link = band_href.split("'")[1]
                band_page_links.append(band_link)

            total_records = int(json_data["iTotalRecords"])
            page_qty = math.floor(total_records / 500)

            for page in range(page_qty):
                page_number += 1
                query_interval += 500
                base_url = f'https://www.metal-archives.com/browse/ajax-letter/l/{letter}/json/1?sEcho={page_number}&iDisplayStart={query_interval}&iSortCol_0=0&sSortDir_0=asc'

                response = requests.get(base_url, headers=headers)
                json_data = json.loads(response.text)

                aaData = json_data.get('aaData', [])
                for band_info in aaData:
                    band_href = band_info[0]
                    band_link = band_href.split("'")[1]
                    band_page_links.append(band_link)
                    
        return band_page_links

    def get_all_band_links(self, headers):
        
        # alphabet = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z','NBR','~']
        alphabet = ["Q", "J"]

        all_band_links = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_letter = {executor.submit(self.get_band_page_links, headers, letter): letter for letter in alphabet}

            # Create a tqdm progress bar for the letters
            for future in concurrent.futures.as_completed(future_to_letter):
                letter = future_to_letter[future]
                band_links = future.result()
                all_band_links.extend(band_links)
                print(f"\r>>> Fetching links for letter {letter}", end="", flush=True)
            
            print("\r>>> Fetching band links completed.")
                
        return all_band_links
    
    def get_band_info(self, headers, band_url):
        response = requests.get(band_url, headers=headers)
        soup = BeautifulSoup(response.content, self.parser)

        band_data = {}

        band_id = band_url.split('/')[-1]
        band_data['Band ID'] = band_id

        band_name_tag = soup.find('h1', class_='band_name')
        if band_name_tag:
            band_name = band_name_tag.a.string.strip()
            band_data['Band Name'] = band_name

        country_tag = soup.find('dt', string='Country of origin:')
        if country_tag:
            band_data['Country of origin'] = country_tag.find_next('dd').text.strip()

        location_tag = soup.find('dt', string='Location:')
        if location_tag:
            band_data['Location'] = location_tag.find_next('dd').text.strip()

        status_tag = soup.find('dt', string='Status:')
        if status_tag:
            band_data['Status'] = status_tag.find_next('dd').text.strip()

        formed_tag = soup.find('dt', string='Formed in:')
        if formed_tag:
            band_data['Formed in'] = formed_tag.find_next('dd').text.strip()

        genre_tag = soup.find('dt', string='Genre:')
        if genre_tag:
            band_data['Genre'] = genre_tag.find_next('dd').text.strip()

        themes_tag = soup.find('dt', string='Themes:')
        if themes_tag:
            band_data['Themes'] = themes_tag.find_next('dd').text.strip()

        last_label_tag = soup.find('dt', string='Last label:')
        if last_label_tag:
            band_data['Last label'] = last_label_tag.find_next('dd').text.strip()

        years_active_tag = soup.find('dt', string='Years active:')
        if years_active_tag:
            band_data['Years active'] = years_active_tag.find_next('dd').text.strip()

        return band_data

    def get_all_band_info(self, headers, all_band_links):
        all_band_info = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_band = {executor.submit(self.get_band_info, headers, link): link for link in all_band_links}

            # Create a tqdm progress bar for the band info
            with tqdm(total=len(all_band_links), position=0, leave=True, dynamic_ncols=True) as pbar:
                for future in concurrent.futures.as_completed(future_to_band):
                    band_link = future_to_band[future]
                    band_info = future.result()
                    all_band_info.append(band_info)
                    
                    # Update the progress bar with the number of band info scraped so far
                    pbar.set_description(f"Scraping band info: {len(all_band_info)}/{len(all_band_links)}")
                    pbar.update(1)  # Update the progress bar

        return all_band_info



    def get_album_info(self, id):
        base_url = f'https://www.metal-archives.com/band/discography/id/{id}/tab/all'
        response = requests.get(base_url)
        soup = BeautifulSoup(response.content, self.parser)
        
        albums = []
        
        table = soup.find('table', class_='display discog')
        if table:
            for row in table.find_all('tr'):
                columns = row.find_all(['td', 'th'])
                if len(columns) == 4:
                    album_name_tag = columns[0].find('a')
                    print(album_name_tag)
                    if album_name_tag:
                        album = {
                            'id': id,
                            'album_id': album_name_tag['href'].split('/')[-1],
                            'name': album_name_tag.text.strip(),
                            'type': columns[1].text.strip(),
                            'year': columns[2].text.strip(),
                            'reviews': columns[3].text.strip(),
                            'album_url': album_name_tag['href']
                        }
                        albums.append(album)
        
        return albums

    def get_all_albums_info(self, all_band_links):
        all_albums_info = []

        ids_list = [link.split("/")[-1] for link in all_band_links]

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_album = {executor.submit(self.get_album_info, id): id for id in ids_list}

            for future in concurrent.futures.as_completed(future_to_album):
                album_link = future_to_album[future]
                album_info = future.result()
                all_albums_info.append(album_info)

        return [item for sublist in all_albums_info for item in sublist]
    
    def get_song_info(self, album_url):
        response = requests.get(album_url)
        soup = BeautifulSoup(response.content, self.parser)

        songs = []

        table = soup.find('table', class_='display table_lyrics')
        if table:
            for row in table.find_all('tr'):
                columns = row.find_all(['td', 'th'])
                song_name_tag = columns[0].find('a')
                if song_name_tag:
                    song = {
                        'album_id': album_url.split('/')[-1],
                        'id': song_name_tag.get('name'),
                        'name': columns[1].text.strip(),
                        'length': columns[2].text.strip(),
                        # 'lyrics': self.get_song_lyrics(song_name_tag.get('name'))
                    }
                    songs.append(song)
        
        return songs
    
    def get_all_songs_info(self, all_albums_info):
        all_songs_info = []

        album_urls = [album['album_url'] for album in all_albums_info]

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_song = {executor.submit(self.get_song_info, url): url for url in album_urls}

            for future in concurrent.futures.as_completed(future_to_song):
                album_url = future_to_song[future]
                song_info = future.result()
                all_songs_info.append(song_info)

        return [item for sublist in all_songs_info for item in sublist]

    def get_song_lyrics(self, song_id):
        url = f"https://www.metal-archives.com/release/ajax-view-lyrics/id/{song_id}"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, self.parser)
        lyrics = soup.get_text('\n')
        return lyrics.strip()
    
    def convert_columns_to_snake_case(self, df):
        new_columns = []
        for column in df.columns:
            # Remove spaces and replace with underscores
            snake_case_column = column.replace(' ', '_')
            # Convert to lowercase
            snake_case_column = snake_case_column.lower()
            new_columns.append(snake_case_column)
        df.columns = new_columns

    def create_dataframe(self, info_list):
        df = pd.DataFrame(info_list)
        self.convert_columns_to_snake_case(df)
        print("DataFrame created with scraped info")
        return df


    # DATABASE METHODS
    def connect_to_database(self, database):
        # Connect to the database
        self.conn = duckdb.connect(database=database, read_only=False)

    def create_schema(self):
        # Create a schema named "raw"
        self.conn.execute('CREATE SCHEMA IF NOT EXISTS raw;')

    def create_table_raw_bands(self):
        # Create a table named "bands" in the "raw" schema
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS raw.band (
                id VARCHAR,
                name VARCHAR,
                country VARCHAR,
                location VARCHAR,
                status VARCHAR,
                formed_in VARCHAR,
                genre VARCHAR,
                themes VARCHAR,
                last_label VARCHAR,
                years_active VARCHAR,
                PRIMARY KEY (id)
            );
        ''')  
        
    def create_table_raw_albums(self):
        # Create a table named "albums" in the "raw" schema
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS raw.album (
                band_id VARCHAR,
                id VARCHAR,
                name VARCHAR,
                type VARCHAR,
                year VARCHAR,
                reviews VARCHAR,
                PRIMARY KEY (band_id, id)
            );
        ''')

    def create_table_raw_songs(self):
        # Create a table named "song" in the "raw" schema
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS raw.song (
                album_id VARCHAR,
                id VARCHAR,
                name VARCHAR,
                length VARCHAR,
                --lyrics VARCHAR,
                PRIMARY KEY (album_id, id)
            );
        ''')

    def insert_band_info(self, df):
        # Gets dataframe variable name
        df_name = f'{df=}'.split('=')[0]
        # Insert the dataframe into the "bands" table
        self.conn.execute(f'''
            TRUNCATE TABLE raw.band;
            ''')
        self.conn.execute(f'''
            INSERT OR IGNORE INTO raw.band
                SELECT
                    band_id,
                    band_name,
                    country_of_origin,
                    location,
                    status,
                    formed_in,
                    genre,
                    themes,
                    last_label,
                    years_active
                FROM {df_name};
        ''')

    def insert_album_info(self, df):
        # Gets dataframe variable name
        df_name = f'{df=}'.split('=')[0]
        # Insert the dataframe into the "bands" table
        self.conn.execute(f'''
            TRUNCATE TABLE raw.album;
            ''')
        self.conn.execute(f'''
            INSERT OR IGNORE INTO raw.album
                SELECT
                    id,
                    album_id,
                    name,
                    type,
                    year,
                    reviews
                FROM {df_name};
        ''')

    def insert_song_info(self, df):
        # Gets dataframe variable name
        df_name = f'{df=}'.split('=')[0]
        # Insert the dataframe into the "bands" table
        self.conn.execute(f'''
            TRUNCATE TABLE raw.song;
            ''')
        self.conn.execute(f'''
            INSERT OR IGNORE INTO raw.song
                SELECT
                    album_id,
                    id,
                    name,
                    length,
                    --lyrics
                FROM {df_name};
        ''')

    def execute_query(self, query):
        # Execute a query and return the result
        result = self.conn.execute(query)
        return result.fetchdf()


    def create_database(self, database):
        self.connect_to_database(database)
        self.create_schema()
        self.create_table_raw_bands()
        self.create_table_raw_albums()
        self.create_table_raw_songs()
        
if __name__ == "__main__":

    headers = {'User-Agent': 'Mozilla/5.0'}
    database = 'metal_db'

    print(">>> Welcome to the Metal Archives Scraper!")
    print(">>> ")
    print(">>> Starting scraping process...")
    print(">>> This may take a while, please wait.")
    print(">>> You can check the progress in the terminal.")
    print(">>> Press CTRL+C to stop the process.")
    print(">>> ")


    scraper = Scraper()

    # Scrape the band info and create a dataframe
    start_time = time.time()
    links = scraper.get_all_band_links(headers)

    band_info = scraper.get_all_band_info(headers, links)
    end_time1 = time.time()    
    print(f">>> Total bands scraping time: {int(end_time1 - start_time)} seconds")

    # albums_info = scraper.get_all_albums_info(links)
    # end_time2 = time.time()
    # print(f">>> Total albums scraping time: {int(end_time2 - end_time1)} seconds")

    # songs_info = scraper.get_all_songs_info(albums_info)
    # end_time3 = time.time()
    # print(f">>> Total songs scraping time: {int(end_time3 - end_time2)} seconds")

    bands_df = scraper.create_dataframe(band_info)
    print(">>> DataFrame created with scraped band info.")

    # albums_df = scraper.create_dataframe(albums_info)
    # print(">>> DataFrame created with scraped albums info.")

    # songs_df = scraper.create_dataframe(songs_info)
    # print(">>> DataFrame created with scraped albums info.")


    # Connect to the database, create the schema and table
    scraper.create_database(database)
    scraper.insert_band_info(bands_df)
    # scraper.insert_album_info(albums_df)
    # scraper.insert_song_info(songs_df)