from html.parser import HTMLParser
from io import StringIO
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import requests
import re

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue().replace(r'\n',' ')

def strip_tags(html):
    #print(html)
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def get_wiki_link(company_name, api_key):
    """
    get company's wikipedia page, if no link retrieve, return None

    Args:
        company_name (str): the company name for query
        api_key (str): API key for scraper API

    Returns:
        (Match Obj) : returning a Match object, or None if no match was found.
    """
    # first use scraper API to get google search result when company name is entered
    url = f'https://www.google.com/search?q={company_name}'
    
    NUM_RETRIES = 10
    params = {'api_key': api_key, 'url': url}
    
    # send request to scraperapi, and automatically retry failed requests
    for _ in range(NUM_RETRIES):
        try:
            response = requests.get(
                'http://api.scraperapi.com/', params=urlencode(params))
            if response.status_code == 200:
                # escape for loop if the API returns a successful response
                break
        except requests.exceptions.ConnectionError:
            response = None
    
    if response is None:
        logging.error(company_name, 'google request failed.')
        return
    
    html_doc = response.text
    # search the wikipedia link of the company
    # one pattern example is https://en.wikipedia.org/wiki/Apple_Inc.
    wiki_link = re.search(r'https://en.wikipedia.org/wiki/[a-zA-Z0-9_.%()]+', html_doc)
    
    return wiki_link

def get_wiki_text(wiki_link):
    """
    get the summary and full text from the wikipedia url

    Args:
        wiki_link (str): the wikipdia url

    Returns:
        tuple: (fulltext, summary), return (None, None) if the no wikipdia page is found
    """

    # get the wikipedia page's html
    NUM_RETRIES = 10
    for _ in range(NUM_RETRIES):
        try:
            response = requests.get(wiki_link)
            if response.status_code == 200:
                break
        except requests.exceptions.RequestException:
            response = None

    if response is None:
        return (None, None)
    
    # use beautiful soup to get the summary part of wikipedia
    html_doc = response.text
    soup = BeautifulSoup(html_doc, 'lxml')
    summary_div = soup.find("div", {"class": "mw-parser-output"})
    
    into_summary = False
    after_summary = False
    summary_text = ''
    if summary_div != None:
            # iterate through the div to get all <p> tags in summary part
            for element in summary_div.contents:
                if into_summary==True:

                    if element.name == 'p':
                        summary_text += str(element)
                        after_summary = True # the element cursor now is pointing to summary <p>
                    
                    # after summary <p> there is always a <div>, but we need to check the div is after the summary
                    # but not div before summay
                    # for example: the structure of html could be:
                    # div p table div div {p p p} div div
                    # we only want the three consecutive <p>
                    if element.name == 'div' and after_summary==True: 
                        break
                if element.name == 'table': # from a wikipedia, above the summary will always be a table
                    into_summary = True
    summary_text = strip_tags(summary_text)
    # get all <p> tags and extract the text
    paragraph_list = soup.findAll('p')

    fulltext = ''
    for paragraph in paragraph_list:
        for element in paragraph:
            fulltext += str(element)
    fulltext = strip_tags(fulltext)

    return fulltext, summary_text