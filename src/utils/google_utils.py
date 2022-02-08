import requests
from bs4 import BeautifulSoup
from dateutil.parser import *
from urllib.parse import urlencode
from newspaper import Article
from newspaper.article import ArticleException

def to_utc_datetime(str_time):
    if(str_time == None or str_time == ''):
        return None
    return parse(str(str_time))


def valid_link(link):
    """
    the urls that need to be excluded due to bad quality or it is not able to
    convert by newspaper3k

    Args:
        link (str): website link

    Returns:
        bool: if the website is valid or not
    """
    if link.startswith(('https://money.usnews.com/investing/stocks',
                        'https://en.wikipedia.org',
                        'https://www.investopedia.com/terms',
                        'https://swingtradebot.com',
                        'https://www.barchart.com',
                        'https://www.marketwatch.com',
                        'https://money.usnews.com/investing/stock-market-news/slideshows/'
                        )):
        return False
    return True


def get_search_result(query, api_key):
    """
    use scraper API to get the urls from google search results.

    Args:
        query (string): keyword
        api_key (str): scraper API Key

    Returns:
        list: list of urls gotten from google search results.
    """
    url = f'https://www.google.com/search?q={query}+stocks'
    NUM_RETRIES = 10
    params = {'api_key': api_key, 'url': url}
    # send request to scraperapi, and automatically retry failed requests
    for _ in range(NUM_RETRIES):
        try:
            response = requests.get(
                'http://api.scraperapi.com/', params=urlencode(params))
            if response.status_code in [200, 404]:
                # escape for loop if the API returns a successful response
                break
        except requests.exceptions.ConnectionError:
            response = ''
    html_doc = response.text
    google_page = BeautifulSoup(html_doc, 'lxml')
    g_div = google_page.findAll('div', {'class': 'g'})
    links = []
    try:
        for element in g_div:
            if element.div.div.div.a != None:
                link = element.div.div.div.a['href']
                if valid_link(link):
                    links.append(link)
    except KeyError:
        pass
    finally:
        return links

def get_website_content(link):
    """
    use newspaper3k to get the contents from the url

    Args:
        link (str): the website url

    Returns:
        text: the text content in the website
    """
    try:
        article = Article(link)
        article.download()
        article.parse()
        article.nlp()
    except ArticleException:
        return None
    return article


def get_sector(tickers, stocks, trim=0):
    """
    the get sector function only for google, because it will use a bigger candidate list
    """
    SIMILARITY = 1/2 # threshold
    threshold = SIMILARITY*len(tickers)
    
    # find industry for all tickers
    records = []
    div = 10**trim
    for ticker in tickers:
        record = stocks[stocks['ticker'] == ticker]['sector']
        records.append(record.astype(int).iloc[0].item(
        )//div if (record.empty == False and record.isna().iloc[0] == False) else None)
    
    # most popular industry
    sector = records[0]
    max_count = records.count(sector)
    for record in records:
        count = records.count(record)
        if count > max_count:
            sector = record
            max_count = count
    
    # test threshold
    if max_count > threshold:
        return sector
    elif trim == 6:
        return None
    else:
        return get_sector(tickers, stocks, trim=trim+2)