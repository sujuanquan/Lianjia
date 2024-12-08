import random
from typing import OrderedDict
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient,errors
import time
from info import city_info

class LianjiaCrawler(object):
    def __init__(self):
        #连接MongoDb
        self.client = MongoClient('mongodb://127.0.0.1:27017')
        self.db = self.client['Lianjia']
        self.collection = self.db['zufang_']
        self.collection.create_index('title', unique=True)


    def crawl(self):

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0',
            'Mozilla/5.0 (Windows NT 6.1; rv:52.0) Gecko/20100101 Firefox/52.0',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:25.0) Gecko/20100101 Firefox/25.0',
        ]

        for city, info in city_info.items():

            for dist, dist_info in info[1].items():
                #在每个城市的每个区爬取指定数目的数据
                page = 1
                dup= 1
                con = 0
                while con < 50:

                    url = f'https://{info[0]}.lianjia.com/zufang/{dist_info}/pg{page}/'
                    content = {
                    'user-agent':random.choice(user_agents),
                    'accept-language':'zh-CN,zh;q=0.9',
                    'Referer': 'https://www.lianjia.com/',
                    'cookie':"lianjia_uuid=357b66e7-66b6-413c-9738-cbd8b5270878; _ga=GA1.2.1748617029.1732437629; _ga_QJN1VP0CMS=GS1.2.1732438000.1.0.1732438000.0.0.0; _ga_KJTRWRHDL1=GS1.2.1732438000.1.0.1732438000.0.0.0; select_city=120000; _jzqckmp=1; GUARANTEE_POPUP_SHOW=true; _gid=GA1.2.788638118.1732760564; GUARANTEE_BANNER_SHOW=true; digv_extends=%7B%22utmTrackId%22%3A%2221583074%22%7D; lianjia_ssid=be122126-f1bc-472b-9d8a-3a0d35f7d79b; Hm_lvt_46bf127ac9b856df503ec2dbf942b67e=1732437617,1732589449,1732760551,1732793235; Hm_lpvt_46bf127ac9b856df503ec2dbf942b67e=1732793235; HMACCOUNT=0E333EB3C8D3853E; _jzqa=1.2766508295865157000.1732437621.1732760551.1732793236.4; _jzqc=1; _jzqy=1.1732437621.1732793236.3.jzqsr=baidu|jzqct=%E9%93%BE%E5%AE%B6.jzqsr=baidu|jzqct=%E9%93%BE%E5%AE%B6; _qzja=1.827148278.1732437621526.1732760551461.1732793236602.1732760551461.1732793236602.0.0.0.4.4; _qzjc=1; _qzjto=2.2.0; _jzqb=1.1.10.1732793236.1; _qzjb=1.1732793236602.1.0.0.0; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221935d543e86220-08d25770247023-26011851-1395396-1935d543e87109b%22%2C%22%24device_id%22%3A%221935d543e86220-08d25770247023-26011851-1395396-1935d543e87109b%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E4%BB%98%E8%B4%B9%E5%B9%BF%E5%91%8A%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22https%3A%2F%2Fwww.baidu.com%2Fother.php%22%2C%22%24latest_referrer_host%22%3A%22www.baidu.com%22%2C%22%24latest_search_keyword%22%3A%22%E9%93%BE%E5%AE%B6%22%2C%22%24latest_utm_source%22%3A%22baidu%22%2C%22%24latest_utm_medium%22%3A%22pinzhuan%22%2C%22%24latest_utm_campaign%22%3A%22wytj%22%2C%22%24latest_utm_content%22%3A%22biaotimiaoshu%22%2C%22%24latest_utm_term%22%3A%22biaoti%22%7D%7D; _gat=1; _gat_past=1; _gat_global=1; _gat_new_global=1; _gat_dianpu_agent=1; _ga_B3G62E46BE=GS1.2.1732793248.4.0.1732793248.0.0.0; _ga_049GGDBYWQ=GS1.2.1732793248.4.0.1732793248.0.0.0; hip=EmnoeBKUDMBxAhj5cGYrwZZ2q7VJrHtNnHnKnFRrBbIxTRCJMO4h8CsPW4ogJi-O4K21YparSTPrM4SWwh5GKhi3YmCZ5EIwPTS4lqNNFkLV6xLadwdMxcJcl5jWGv_qDruevd8eKaRrs8w85ytGiQCBZjH3Jv2zOmAvNOtfpeR_dyL7dHzUVW4Dpg%3D%3D; srcid=eyJ0Ijoie1wiZGF0YVwiOlwiYmY1ZTU0ODRmYzY4MTI5MThlMGMxMjM5YjhiMDNlZWFkZjc2MTQ4ZGVmYjIzNzE1YzdkYmRiYjZhMzI1YWQwZmE0YWQ3ZDNkNDNiZWRmYWJiYmMzMTE3NDViYzdkMmUxOTM1MTBhZDFmMDNkN2NkMjBlYzY4Mjg1ZDE4ZWEyMWUxM2ViOTBmZjIyNjc0NDg1MzhkMDUxZjUxNTRhYTBmY2JkNDIyMDU3NTI3YjI5YTdhNTViNzY3NGY3YjFhZGE5NjA5ZmZhOTRlYjk2YWNmYThkMjhjNTkwNDJjZDZhNDIyMjEzOGZhNDkxMTNhY2MwMTQwZGEwZTUxZWNiODM4OVwiLFwia2V5X2lkXCI6XCIxXCIsXCJzaWduXCI6XCI3NzUxOTVjYVwifSIsInIiOiJodHRwczovL3RqLmxpYW5qaWEuY29tL3p1ZmFuZy8iLCJvcyI6IndlYiIsInYiOiIwLjEifQ=="
                    }

                    req = requests.get(url,headers=content)
                    content_ = req.content

                    item = {'city':city,'dist':dist}
                    dup = self.get_content(content_,item,dup)
                    if dup == 0:
                        print(f"{city}市{dist}区已无更多数据")
                        break
                    page += 1
                    con += 1
                    print(f"已爬取{con*30}条数据！")
                    time.sleep(random.randint(2,8))



    def get_content(self, content,item,dup):

        #从爬取的数据中提取想要的内容储存到MongoDB
        soup = BeautifulSoup(content,'lxml')
        divs = soup.find_all('div',class_='content__list--item--main')

        print(f"找到 {len(divs)} 个div标签")
        if len(divs) < 10:
            #说明没有更多数据了
            dup = 0

        for div in divs:

            title_tag = div.find('a', class_='twoline')
            if title_tag:
                item['title'] = title_tag.text.strip()
            else:
                item['title'] = "None"
            if item['title'] == "None":
                continue

            p_tags = div.find_all('p', class_='content__list--item--des')
            item['address'] = ' '.join([a.get_text().strip() for p in p_tags for a in p.find_all('a')])
            if not item['address']:
                item['address'] = "None"
            item['area'] = "None"
            item['room_type'] = "None"
            item['direction'] = "None"
            item['floor_info'] = "None"
            item['rem_word'] = "None"
            item['price'] = "None"

            for p_tag in p_tags:
                texts = [text.strip() for text in p_tag.find_all(text=True) if text.strip()]
                for text in texts:
                    if '㎡' in text:
                        item['area'] = text.strip()  # 面积
                    elif '室' in text and '厅' in text:
                        item['room_type'] = text.strip()  # 房型
                    elif len(text) == 1 and text.isalpha():
                        item['direction'] = text.strip()  # 方向

            item['floor_info'] = soup.find('span', class_='hide').text.replace('\n', ' ').replace('/','').replace('                        ',' ').strip()
            item['rem_word'] = div.find('p',class_='content__list--item--bottom oneline').text.replace('\n', ' ').strip()
            item['price'] = div.find('span',class_='content__list--item-price').text.strip()

            o_item = OrderedDict[str, str]([
                ('title', item['title']),
                ('city', item['city']),
                ('dist', item['dist']),
                ('address', item['address']),
                ('area', item['area']),
                ('room_type', item['room_type']),
                ('direction', item['direction']),
                ('floor_info', item['floor_info']),
                ('rem_word', item['rem_word']),
                ('price', item['price'])
            ])
            item_ = dict(o_item)

            self.collection.update_one({'title': item_['title']}, {'$set': item_}, upsert=True)
            print('成功保存数据:{}!'.format(item_))

        return dup


if __name__ == '__main__':
    crawler = LianjiaCrawler()
    crawler.crawl()
