
from scrapy.http import TextResponse
from fake_useragent import UserAgent
import re
import time
import pandas as pd

from sqlalchemy import *
import MySQLdb
import configparser


class Emart:
    def __init__(self):
        self.test_cate = ["6000095695"]
        self.categories = ["6000097651","6000097652","6000097653","6000097640","6000097639","6000097702","6000097703","6000097704","6000097705","6000097706","6000097707","6000097656","6000097658","6000097669","6000097671","6000097670",'6000095598',"6000095711","6000095727","6000095709", "6000095605","6000095607","6000095608","6000095604","6000095724","6000095606","6000095712","6000095726","6000095728","6000095729","6000095713","6000095714","6000095715", "6000095591","6000095590","6000095592","6000095594","6000095609","6000095725","6000095612","6000095593", "6000095595","6000097134", "6000097196","6000097637","6000097136","6000095508","6000097138","6000097137","6000095862","6000095506","6000096326", "6000097506", "6000097507", "6000095796", "6000095794", "0006510363","0006510361","0006510364","0006510365","0006510366","0006510546", "0006510362","0006510367","6000095501", "6000095740", "6000095498", "6000095500", "6000095499"]
    
    
    
    def last_page(self, category):
        url = "http://emart.ssg.com/disp/category.ssg?dispCtgId={}".format(category)
        req = requests.get(url, headers={"User-Agent": UserAgent().chrome})
        time.sleep(5)
        response = TextResponse(req.url, body=req.text, encoding="utf-8")

        try:
            last = response.xpath('//*[@id="area_itemlist"]/div[2]/a/@onclick').extract()[-1]
            last_page = int(re.findall('\d+', last)[0]) + 1
            print("끝페이지 : ", last_page-1)

        except:
            last_page = 2
            print("끝페이지: ", last_page-1)

        return last_page

    
    
    def emart_crawling(self):
        dfs = pd.DataFrame()

        for category in self.categories:
            print("=== 카테고리 번호 : ", category, " ===")
            last_page = self.last_page(category)

            for p in range(1, last_page):
                url = "http://emart.ssg.com/disp/category.ssg?dispCtgId={}&page={}".format(category, p)

                req = requests.get(url, headers={"User-Agent": UserAgent().chrome})
                time.sleep(5)
                response = TextResponse(req.url, body=req.text, encoding="utf-8")

                elements = response.xpath('//*[@id="ty_thmb_view"]/ul/li')
                print("페이지 번호", p, "\t", "상품 수 : ", len(elements))

                # category
                cate = [category]*len(elements)

                # title
                titles = response.xpath('//*[@id="ty_thmb_view"]/ul/li/div[@class="cunit_info"]/div[@class="cunit_md notranslate"]/div[@class="title"]/a/em[@class="tx_ko"]/text()').extract()

                # price
                price = response.xpath('//*[@id="ty_thmb_view"]/ul/li/div[@class="cunit_info"]/div[@class="cunit_price notranslate"]/div[@class="opt_price"]/em[@class="ssg_price"]/text()').extract()

                # link
                links = response.xpath('//*[@id="ty_thmb_view"]/ul/li/div[1]/div[2]/a/@href').extract()
                links = [response.urljoin(link) for link in links]


                # price per gram
                idx = range(0,len(elements))
                pprs = []
                for n in idx:
                    ppr = response.xpath('//*[@id="ty_thmb_view"]/ul/li[{}]/div[@class="cunit_info"]/div[@class ="cunit_prw"]/div/text()'.format(n)).extract()
                    ppr = ["-" if ppr == [] else ppr[0]]
                    pprs.append(ppr[0])

                df = pd.DataFrame({"category": cate, "titles": titles, "price": price, "links": links, "price/gram": pprs})
                dfs = pd.concat([dfs, df])

        return dfs

        
        
    def min_gram(self, data):

        datas = data.replace(" ", "")

        if re.findall('(\d+.?\d?[l|L]\*?x?X?\d?\d?)', datas):
            datas = re.findall('(\d+.?\d?[l|L]\*?x?X?\d?\d?)', datas)[::-1][0]

        elif re.findall('(\d+.?\d?k?K?[g|G]\*?x?X?\d?\d?)', datas):
            datas = re.findall('(\d+.?\d?k?K?[g|G]\*?x?X?\d?\d?)', datas)[::-1][0]

        else:
            datas = "-"

        return datas
    
    

    def category_name(self, category):
        names = {"냉동상품": ["6000097651", "6000097652", "6000097653", "6000097640", "6000097639", "6000097702", "6000097703", "6000097704", "6000097705", "6000097706", "6000097707"],
                 "냉장식품": ["6000097656", "6000097658", "6000097669", "6000097671", "6000097670"],
                 "식용유/설탕/소금": ['6000095598', "6000095711", "6000095727", "6000095709"],
                 "장류/조미료": ["6000095605", "6000095607", "6000095608", "6000095604", "6000095724", "6000095606", "6000095712", "6000095726", "6000095728", "6000095729", "6000095713", "6000095714", "6000095715"],
                 "소스/양념/육수": ["6000095591", "6000095590", "6000095592", "6000095594", "6000095609", "6000095725", "6000095612", "6000095593"],
                 "밀가루/라면/믹스류": ["6000095595", "6000097134"],
                 "편의식/가공기타": ["6000097196", "6000097637", "6000097136", "6000095508", "6000097138", "6000097137", "6000095862", "6000095506", "6000096326"],
                 "반찬류": ["6000097506", "6000097507"],
                 "과일": ["6000095796", "6000095794"],
                 "포장용기": ["0006510363", "0006510361", "0006510364", "0006510365", "0006510366", "0006510546"],
                 "가게위생용품": ["0006510362", "0006510367"],
                 "유제품": "6000095501",
                 "야채/채소": "6000095740",
                 "쌀/잡곡/견과": "6000095498",
                 "수산/건어물": "6000095500",
                 "축산/계란": "6000095499"}
        for name, num in names.items():
            if num == category:
                return name

            
            
    def sql_db(self):
        config = configparser.ConfigParser()
        config.read(".kw_sql.ini")
        sql_datas = config["sql"]

        engine = create_engine("mysql://{}:{}@{}/crawling?charset=utf8".format(sql_datas["user"], sql_datas["pw"], sql_datas["public_ip"]), encoding='utf8')

        dfs = self.emart_crawling()
        dfs["min_gram"] = dfs["titles"].apply(lambda data: self.min_gram(data))
        dfs = dfs.reset_index(drop=True)
        dfs.to_sql(name="emart_goods", con=engine, if_exists="replace")
        print("saved in mySQL database!!")
        
        return dfs
