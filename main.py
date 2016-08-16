# -*- coding:utf-8 -*-
# Created by yanlei on 16-8-16 at 下午2:23
import logging

import requests
import lxml.html
import pymongo

sess = requests.Session()
connection = pymongo.MongoClient('127.0.0.1', 27017)

db = connection['douban']
collection = db['book']

base_url = 'https://book.douban.com/tag/?icn=index-nav'
host_url = 'https://book.douban.com'

user_agent = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0'
headers = {'User-Agent': user_agent}

sess.proxies = {'http': 'http://117.57.56.60:8998'}
sess.headers = headers
logging.basicConfig(level=logging.DEBUG)


def download_url(page_url):
    try:
        response = sess.get(page_url)
        if response.status_code == 200:
            return response.content
        else:
            return None
    except IOError as e:
        print e
        return None


def convert_content_to_html(content):
    if content:
        document = lxml.html.fromstring(content)
        return document
    else:
        return None


def parse_home_page(home_url):
    content = download_url(home_url)
    document = convert_content_to_html(content)
    if document:
        tag_urls = document.xpath('//table[@class="tagCol"]/tbody/tr/td/a/@href')
        for tag_url in tag_urls:
            yield tag_url


def parse_book_url(tag_url):
    logging.info("Parsing {0}".format((host_url + tag_url).encode('utf-8')))
    content = download_url(host_url + tag_url)
    document = convert_content_to_html(content)
    books_doc = document.xpath('//div[@class="info"]')
    for book_doc in books_doc:
        book_name = book_doc.xpath('h2[@class=""]/a/@title')
        book_url = book_doc.xpath('h2[@class=""]/a/@href')
        book_score = book_doc.xpath('div[@class="star clearfix"]/span[@class="rating_nums"]/text()')
        if len(book_name) and len(book_url) and len(book_score):
            book_doc = {"name": book_name[0],
                        "score": book_score[0],
                        "url": book_url[0]}
            yield book_doc
    next_page_url = document.xpath('//span[@class="next"]/a/@href')
    if len(next_page_url):
        logging.info("Parsing next page: {}".format(next_page_url[0].encode('utf-8')))
        parse_book_url(next_page_url[0])


def parse_book():
    for tag_url in parse_home_page(base_url):
        logging.info("Parsing {}".format(tag_url.encode('utf-8')))
        for book in parse_book_url(tag_url):
            logging.info("Get book: book name:{0} book score {1}".format(book['name'].encode('utf-8'), book['score']))
            yield book


def store_into_mongodb(item):
    if not collection.find({"name": item['name']}):
        collection.insert(item)
        logging.info("Store into mongodb")


if __name__ == "__main__":
    for book_item in parse_book():
        store_into_mongodb(book_item)
