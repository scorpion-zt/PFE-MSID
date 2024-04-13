#Download The driver necessary for the Web Browser
#pip install pandas
#pip install selenium
#pip install beautifulsoup4

from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd

from subprocess import PIPE, Popen

driver = webdriver.Chrome("/Users/mac/Downloads/chromedriver")

# *********** Real News ***********

def get_urls_domaines_for_real_news():

    url="https://www.mapnews.ma/fr/"

    urls_Domaines = []

    driver.get(url)
    content = driver.page_source
    soup = BeautifulSoup(content, features="html.parser")
    
    for i in soup.find_all('li', attrs={'class':'tb-megamenu-item level-2 mega'}):
        urls_Domaines.append("https://www.mapnews.ma"+str(i.find('a')['href']))

    return urls_Domaines

def get_nbr_last_page_for_real_news(url_id):

    driver.get(url_id)
    content = driver.page_source
    soup = BeautifulSoup(content, features="html.parser")
    
    last_page_link = soup.find('li', attrs={'class':'pager-last last'})    
    last_page_url = last_page_link.find('a')['href']
    last_page_number = last_page_url.split('=')[-1]
    
    return last_page_number

def parse_pages_get_real_news():

    news = []

    for x in get_urls_domaines_for_real_news():
    
        unique_news = set()

        for i in range(0,int(get_nbr_last_page_for_real_news(x))+1):
            url=str(x)+"?page="+str(i)
            driver.get(url)
            content = driver.page_source
            soup = BeautifulSoup(content, features="html.parser")
            
            all_div_for_this_page = soup.find_all('div', attrs={'class':'block-1'})

            for div in all_div_for_this_page:
                div_ID = div.find('div', attrs={'class':lambda x: x and x.startswith('titre-actualites-aj-top-categorie')})
                if div_ID:
                    span_ID = div_ID.find('span', attrs={'class':'field-content'})
                    if span_ID:
                        a_element_ID = span_ID.find('a')
                        if a_element_ID:
                            news_ID = str(a_element_ID.text.strip())
                            if news_ID not in unique_news:
                                news.append(news_ID)
                                unique_news.add(news_ID)
    
    news_with_origin = [(article, "Real News") for article in news]

    return news_with_origin

# *********** Fake News ***********

def get_nbr_last_page_for_fake_news():

    url="https://www.mapnews.ma/fr/cat%C3%A9gorie/fake-news"
    driver.get(url)
    content = driver.page_source
    soup = BeautifulSoup(content, features="html.parser")
    
    last_page_link = soup.find('li', attrs={'class':'pager-last last'})    
    last_page_url = last_page_link.find('a')['href']
    last_page_number = last_page_url.split('=')[-1]

    
    return last_page_number

def parse_pages_get_fake_news():

    news = []

    for i in range(0,int(get_nbr_last_page_for_fake_news())+1):

        url="https://www.mapnews.ma/fr/cat%C3%A9gorie/fake-news?page="+str(i)
        driver.get(url)
        content = driver.page_source
        soup = BeautifulSoup(content, features="html.parser")
        
        all_div_for_this_page = soup.find_all('div', attrs={'class':'node-tpl-title'})

        for div in all_div_for_this_page:
            h2_element = div.find('h2')
            if h2_element:
                news.append(str(h2_element.text.strip()))
    
    news_with_origin = [(article, "Fake News") for article in news]

    return news_with_origin

# *********** Upload csv File to HDFS ***********

def Upload_csv_File_to_HDFS(File_path,HDFS_path):
    put = Popen(["hadoop", "fs", "-put", File_path, HDFS_path], stdin=PIPE, bufsize=-1)
    put.communicate()

# *********** Main Methode ***********

def Main():

    df_fake_news = pd.DataFrame(parse_pages_get_fake_news(), columns=['News', 'Origin'])
    df_real_news = pd.DataFrame(parse_pages_get_real_news(), columns=['News', 'Origin'])

    df_combined = pd.concat([df_fake_news, df_real_news], ignore_index=True)

    df_combined.insert(0, 'ID', range(1, len(df_combined) + 1))

    df_combined['Value'] = [0 if origin == "Fake News" else 1 for origin in df_combined['Origin']]

    df_combined['News'] = df_combined['News'].str.replace(r'\bFAUX\b|\*FAUX\*\.|\bFaux\b', '')
    df_combined['News'] = df_combined['News'].str.replace(r'\s\.$', '')

    df_combined.to_csv('/Users/mac/Downloads/PFE-MSID/Data-Input/data_News.csv', index=False, encoding='utf-8',sep=';')

    Upload_csv_File_to_HDFS('/Users/mac/Downloads/PFE-MSID/Data-Input/data_News.csv','/Input_Data/')

# *********** Run Code : Main() ***********

if __name__ == "__main__":
    Main()