# Code based on Python 3.x

# _*_ coding: utf-8 _*_

# __Author: "LEMON"

from datetime import datetime

from urllib.parse import urlencode

from multiprocessing import Pool

import requests
import random
import time
from bs4 import BeautifulSoup
import pymongo
from zhilian_kw_config import *
import time
from itertools import product
#随机UserAgent
from fake_useragent import UserAgent
ua = UserAgent()
headers = {'User-Agent': ua.random}


client = pymongo.MongoClient(MONGO_URI)

db = client[MONGO_DB]


def download(url):

    response = requests.get(url, headers=headers)

    return response.text


def get_content(html):
    # 记录保存日期

    date = datetime.now().date()

    date = datetime.strftime(date, '%Y-%m-%d')  # 转变成str

    soup = BeautifulSoup(html, 'lxml')

    body = soup.body

    data_main = body.find('div', {'class': 'newlist_list_content'})

    if data_main:

        tables = data_main.find_all('table')

        for i, table_info in enumerate(tables):

            if i == 0:
                continue

            tds = table_info.find('tr').find_all('td')

            zwmc = tds[0].find('a').get_text()  # 职位名称

            zw_link = tds[0].find('a').get('href')  # 职位链接

            fkl = tds[1].find('span').get_text()  # 反馈率

            gsmc = tds[2].find('a').get_text()  # 公司名称

            zwyx = tds[3].get_text()  # 职位月薪

            gzdd = tds[4].get_text()  # 工作地点

            gbsj = tds[5].find('span').get_text()  # 发布日期

            tr_brief = table_info.find('tr', {'class': 'newlist_tr_detail'})


            brief = tr_brief.find('li', {'class': 'newlist_deatil_last'}).get_text()   # 招聘简介

            xinzi = tr_brief.find('li', {'class': 'newlist_deatil_two'}).find_all("span")[1].get_text().replace('公司性质：',
                                                                                                                '')#公司性质

            xueli = tr_brief.find('li', {'class': 'newlist_deatil_two'}).find_all("span")[-2].get_text().replace('学历：',
                                                                                                                 '')#学历

            many_man = tr_brief.find('li', {'class': 'newlist_deatil_two'}).find_all("span")[2].get_text().replace('公司规模：',
                                                                                                                '')#公司规模

            # 用生成器获取信息

            yield {'zwmc': zwmc,  # 职位名称

                   'fkl': fkl,  # 反馈率

                   'gsmc': gsmc,  # 公司名称

                   'zwyx': zwyx,  # 职位月薪

                   'gzdd': gzdd,  # 工作地点

                   'gbsj': gbsj,  # 公布时间

                   'brief': brief,  # 招聘简介

                   'zw_link': zw_link,  # 网页链接

                   'save_date': date,  # 记录信息保存的日期

                   'xinzi': xinzi,    #公司性质

                   'xueli': xueli,     #学历

                   'many_man' :  many_man   #公司规模

                   }


def main(args):
    basic_url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?'

    for keyword in KEYWORDS:

        mongo_table = db[keyword]

        paras = {'jl': args[0],

                 'kw': keyword,

                 'p': args[1]  # 第X页

                 }

        url = basic_url + urlencode(paras)

        # print(url)

        html = download(url)

        # print(html)

        if html:

            data = get_content(html)

            for item in data:

                if mongo_table.update({'zw_link': item['zw_link']}, {'$set': item}, True):
                    print('已保存记录：', item)


if __name__ == '__main__':
    start = time.clock()

    number_list = list(range(TOTAL_PAGE_NUMBER))

    args = product(ADDRESS, number_list)

    pool = Pool()

    pool.map(main, args)  # 多进程运行

    end = time.clock()

    print('\n','程序结束',' 共运行了 ',str((end-start)/60),' 分钟')