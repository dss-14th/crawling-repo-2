
import pandas as pd
import requests 
import time

categories = {
    100015 : "냉동상품",
    100024 : "냉장식품",
    100028 : "유제품",
    100032 : "식용유/설탕/소금",
    100038 : "장류/조미료",
    100043 : "소스/양념/육수",
    100050 : "밀가루/라면/믹스류",
    100054 : "편의식/가공기타",
    100061 : "반찬류",
    100067 : "야채/채소",
    100077 : "과일",
    100081 : "쌀/잡곡/견과",
    100085 : "수산/건어물",
    100096 : "축산/계란",
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

# global gagong_df -> 파이썬 파일을 실행 했을 때 gagong_df가 나왔으면 좋겠음.. 
baemin_mart_items_df = pd.concat(dfs, ignore_index=True)
baemin_mart_items_df

baemin_mart_items.to_csv("baemin_mart_items.csv", index=False, encoding="utf-8-sig")

print("="*100)
print("It's saved as baemin_mart_items.csv file.")
print("="*100)
