import requests
from bs4 import BeautifulSoup
import datetime
import sqlite3
import time
import tqdm

class HankyungCrawler():
    def __init__(self, db_name='hankyung.db'):
        self.db_name = db_name
        self.con = sqlite3.connect(self.db_name)

        # check whether there is table named "news"
        self.cursor = self.con.cursor()
        self.cursor.execute('SELECT name FROM sqlite_master WHERE type=\'table\'')
        tables = self.cursor.fetchone()
        if tables == None or 'news' not in tables:
            self.cursor.execute('CREATE TABLE news (date text, field text, title text, content text, url text)')
            self.con.commit()

    def parse_news(self, url):
        result = requests.get(url)
        bs_obj = BeautifulSoup(result.content, 'html.parser')
        title = bs_obj.title.string
        content_obj = bs_obj.find('div', {'id':'articletxt'})
        import re
        def cleanhtml(raw_html):
            cleanr = re.compile('<.*?>|\s{2}|\n')
            cleantext = re.sub(cleanr, '', raw_html)
            return cleantext
        content = cleanhtml(str(content_obj))
        return title, content


    def update(self, numdays): # update db file. crawl news since numdays ago
        base = datetime.datetime.today()
        date_list = [base - datetime.timedelta(days=x) for x in range(numdays)]
        for date in date_list:

            date_str = date.strftime('%Y/%m/%d')
            result = requests.get('https://www.hankyung.com/sitemap/'+date_str)
            print('crawling news of {}'.format(date_str))
            bs_obj = BeautifulSoup(result.content, 'html.parser')
            raw_title_list = bs_obj.find_all('h1', attrs={'class':'news_tit'})
            for raw_title in tqdm.tqdm(raw_title_list):
                url = raw_title.a['href']
                if self.check_url_stored(url):
                    continue

                title, content = self.parse_news(url)
                field = url.split('.com/')[1].split('/')[0]
                self.cursor.execute('INSERT INTO news VALUES (?,?,?,?,?)', (date_str, field, title, content, url))
        self.con.commit()

    def update_threaded(self, numdays, thread_num): # update using multi-threading
        from threading import Thread

        base = datetime.datetime.today()
        date_list = [base - datetime.timedelta(days=x) for x in range(numdays)]
        target_url_and_dates = []
        for date in date_list:

            date_str = date.strftime('%Y/%m/%d')
            result = requests.get('https://www.hankyung.com/sitemap/' + date_str)
            print('crawling news of {}'.format(date_str))
            bs_obj = BeautifulSoup(result.content, 'html.parser')
            raw_title_list = bs_obj.find_all('h1', attrs={'class': 'news_tit'})
            for raw_title in raw_title_list:
                url = raw_title.a['href']
                if self.check_url_stored(url):
                    continue
                target_url_and_dates.append((url, date_str))

        news_list = []

        def parse_and_insert(url_and_dates):
            for url, date_str in tqdm.tqdm(url_and_dates):
                title, content = self.parse_news(url)
                field = url.split('.com/')[1].split('/')[0]
                news_list.append((date_str, field, title, content, url))

        workers = []
        for i in range(thread_num):
            url_length = len(target_url_and_dates) // thread_num
            if i == thread_num-1: # last thread processes remaining urls.
                thread = Thread(target=parse_and_insert, args=(target_url_and_dates[url_length * i:],))
            else:
                thread = Thread(target=parse_and_insert, args=(target_url_and_dates[url_length*i:url_length*(i+1)],))
            workers.append(thread)
            # thread.start()
        for worker in workers:
            worker.start()
            time.sleep(0.1)

        for worker in workers:
            worker.join()

        self.cursor.executemany('INSERT INTO news VALUES (?,?,?,?,?)', news_list)
        self.con.commit()

    def check_url_stored(self, url): # check url is in database
        self.cursor.execute('SELECT * FROM news WHERE url=(?)',(url,))
        result = self.cursor.fetchall()
        return result

    def get(self): # get all news in database
        self.cursor.execute('SELECT * FROM news')
        result = self.cursor.fetchall()
        return result

    def delete_records(self): # delete all new in database
        self.cursor.execute('DELETE FROM news')
        self.con.commit()

if __name__ == "__main__":
    hankyung = HankyungCrawler()
    # hankyung.delete_records()
    hankyung.update(2)
    # hankyung.update_threaded(2,4)
    news_list = hankyung.get()
    print('hankyung.db 내의 뉴스 개수: {}'.format(len(news_list)))
    print('뉴스 샘플')
    print(news_list[0])
