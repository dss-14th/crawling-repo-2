
import pandas as pd
import requests 
import time
from sqlalchemy import *
import configparser

config = configparser.ConfigParser()
config.read("../db.ini")
sql_datas = config["sql"]

# 카테고리 수정 전(참고용)
# categories = {
#     100104 : "플라스틱용기",
#     100105 : "종이용기/박스",
#     100138 : "카페/음료용기",
#     100159 : "종이/비닐봉투",
#     100165 : "수저/위생용품",
#     100177 : "스티커/배너/포스터",
#     100184 : "포장재",
#     100188 : "기타가게용품",
# }

# 카테고리 수정 후 
categories = {
    100104 : "포장용품",
    100105 : "포장용품",
    100138 : "포장용품",
    100159 : "포장용품",
    100165 : "가게위생용품",
    100184 : "포장용품",
    100188 : "가게위생용품",
}


def baemin_items(category, page):
    url = "https://mart.baemin.com/api/v2/front/categories/goods/{}/paging?goodsSortType=BASIC&page={}".format(category, page)
    response = requests.get(url)
    elements = response.json()['data']['simpleGoodsDtoPage']['content']
    datas = []
    for element in elements:
        title = element['name']
        price = element['goodsPrice']
#         o_price = element['customerPrice']
        # 원인은 모르겠지만.. 100050/밀가루 쪽에서 에러남
        try :
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
            "title" : title,
            "price" : price,
#             "o_price" : o_price,
            "min_gram" : min_gram,
            "price_per_gram" : price_per_gram,
            "link" : link,
        })
        
    return pd.DataFrame(datas)

dfs = []
for category in categories:
    print(categories[category])
    for page in range(12): # url에서 page가 0부터 시작
        print(page, end=" ")
        df = baemin_items(category, page)
        dfs.append(df)
    print()
    time.sleep(2)

baemin_mart_delivery_goods_df = pd.concat(dfs, ignore_index=True)
baemin_mart_delivery_goods_df

engine = create_engine("mysql://{}:{}@{}/test?charset=utf8"
                       .format(sql_datas["user"], sql_datas["pw"], sql_datas["public_ip"]), encoding='utf8')
baemin_mart_delivery_goods_df.to_sql(name="baemin_del_goods", con=engine, if_exists="replace")

# baemin_mart_delivery_goods_df.to_csv("baemin_mart_delivery_goods.csv", index=False, encoding="utf-8-sig")

# print("="*100)
# print("It's saved as baemin_mart_delivery_goods.csv file.")
# print("="*100)
