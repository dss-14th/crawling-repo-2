
import pandas as pd
import requests
import time
import configparser
import pymongo
from sqlalchemy import create_engine


config = configparser.ConfigParser()
config.read("../db.ini")
sql_datas = config["sql"]
mongodb = config["mongodb"]

categories = {
    100015: "냉동상품",
    100024: "냉장식품",
    100028: "유제품",
    100032: "식용유/설탕/소금",
    100038: "장류/조미료",
    100043: "소스/양념/육수",
    100050: "밀가루/라면/믹스류",
    100054: "편의식/가공기타",
    100061: "반찬류",
    100067: "야채/채소",
    100077: "과일",
    100081: "쌀/잡곡/견과",
    100085: "수산/건어물",
    100096: "축산/계란",
    100104: "포장용품",
    100105: "포장용품",
    100138: "포장용품",
    100159: "포장용품",
    100165: "가게위생용품",
    100184: "포장용품",
    100188: "가게위생용품",
}

class Baemin:
        
#     def __init__(self):
        
               
    def __baemin_items(self, category, page):
        url = "https://mart.baemin.com/api/v2/front/categories/goods/{}/paging?goodsSortType=BASIC&page={}".format(
            self.category, self.page)
        response = requests.get(url)
        elements = response.json()['data']['simpleGoodsDtoPage']['content']
        datas = []
        for element in elements:
            title = element['name']
            price = element['goodsPrice']
            # 원인은 모르겠지만.. 100050/밀가루 쪽에서 에러남
            try:
                min_gram = element['sizeDesc']
            except:
                min_gram = "-"
            try:
                price_per_gram = element['priceDesc']
            except:
                price_per_gram = "-"
            link = "https://mart.baemin.com/goods/detail/" + str(element['id'])

            datas.append({
                "category": categories[category],
                "title": title,
                "price": price,
                "min_gram": min_gram,
                "price_per_gram": price_per_gram,
                "link": link,
            })

        return pd.DataFrame(datas)
    
    
    def __last_page(self, category):
        url = "https://mart.baemin.com/api/v2/front/categories/goods/{}/paging?goodsSortType=BASIC&page=0".format(
        self.category)
        response = requests.get(url)
        max_pages = response.json()['data']['simpleGoodsDtoPage']['totalPages']
        return max_pages
         

    def crawling_baemin(self, categories, page=0, csv=False, sql=False, mongodb=False):
        dfs = []
        for self.category in categories:
            max_pages = self.__last_page(self.category)
            print("품목 : {}, 총페이지 : {} ".format(categories[self.category], max_pages))
            for self.page in range(max_pages): # url에서 page가 0부터 시작
                print(self.page + 1, end=" ")
                df = self.__baemin_items(self.category, self.page)
                dfs.append(df)
            print()
            time.sleep(2)
        global baemin_df
        baemin_df = pd.concat(dfs, ignore_index=True)
        print("finish crwaling_baemin")
        
        if csv:
            self.__save_csv()
        elif sql:
            self.__db_sql()
        elif mongodb:
            self.__db_mongodb()
        else:
            return baemin_df


    def __save_csv(self):
        baemin_df.to_csv("baemin_df.csv", index=False, encoding="utf-8-sig")
        print("="*60)
        print("baemin data save baemin_df.csv..")
        print("="*60)


    def __db_sql(self):
        engine = create_engine("mysql://{}:{}@{}/test?charset=utf8"
                               .format(sql_datas["user"], sql_datas["pw"], sql_datas["public_ip"]), encoding='utf8')
        baemin_df.to_sql(name="baemin_items", con=engine, if_exists="replace")
        print("="*60)
        print("baemin data is in sql..")
        print("="*60)


    def __db_mongodb(self):
        client = pymongo.MongoClient(
            "mongodb://{}:{}@{}:27017/".format(mongodb["user"], mongodb["pw"], mongodb["public_ip"]))
        baemin_items = client.baemin_db.baemin_items
        items = baemin_df.to_dict('records')
        baemin_items.insert(items)
        print("="*60)
        print("baemin data is in MongoDB..")
        print("="*60)
