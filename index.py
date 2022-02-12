from typing import Collection
import pandas as pd
import json
import pymongo
import glob
import sys
import os
import re

# 載入設定好的es變數
sys.path.append(os.path.abspath("./startup"))
from elasticIndexSetting import es

# 宣告路徑後遍歷.csv
for fname in glob.glob("./companyList/*.csv"):
    # 讀取csv
    df = pd.read_csv(fname)

    # 取出公司狀態為"核准設立"的筆數，並去除不要的欄位(加入營業項目別?)
    fliterCondition = df["公司狀態"] == "核准設立"
    df = df[fliterCondition]
    df = df.filter(["統一編號", "公司名稱", "公司地址", "實收資本額", "公司狀態"])

    # 將營業類別放入DF並將統一編號改為字串
    companyType = fname.replace("公司登記(依營業項目別)－", "").replace(".csv", "")
    companyType = re.sub("[^\u4e00-\u9fa5]+","",companyType) #移除路徑資訊
    df["營業項目別"] = companyType
    df["統一編號"] = df["統一編號"].astype("str")

    # 將dataframe轉json(以每筆資料做單位),之後再轉list
    jsonData = df.to_json(orient="records", force_ascii=False)
    jsonList = json.loads(jsonData)

    # 先使用測試資料
    for index in range(10):
      companyData = {
          "taxID": jsonList[index]["統一編號"],
          "comapnyName": jsonList[index]["公司名稱"],
          "comapnyAddress": jsonList[index]["公司地址"],
          "companyCapital": jsonList[index]["實收資本額"],
          "companySituation": jsonList[index]["公司狀態"],
          "companyType": jsonList[index]["營業項目別"],
      }
      # es新增或更新資料
      es.update(
          index="companies",
          id=jsonList[index]["統一編號"],
          body={
            "doc": companyData, 
            "doc_as_upsert": True,
          },
      )

    # for companyJson in jsonList:
    #   companyData = {
    #       "taxID": companyJson["統一編號"],
    #       "comapnyName": companyJson["公司名稱"],
    #       "comapnyAddress": companyJson["公司地址"],
    #       "companyCapital": companyJson["實收資本額"],
    #       "companySituation": companyJson["公司狀態"],
    #       "companyType": companyJson["營業項目別"],
    #   }
    #   # es新增或更新資料
    #   es.update(
    #       index="companies",
    #       id=companyJson["統一編號"],
    #       body={
    #         "doc": companyData, 
    #         "doc_as_upsert": True,
    #       },
    #   )


    # # 將companyJson根據type存進mongo
    # myclient = pymongo.MongoClient("mongodb://localhost:27017")

    # # 1/10 :用函式建立hash表對應collectionName
    # collectionName=jsonList[index]["營業項目別"] 

    # mydb = myclient["testDB"]
    # mycol = mydb["GeneralAdvertisingServices"] #先寫死
    # mydict = companyData
    # mycol.update_one({"taxID":mydict["taxID"]},{ "$set": mydict } , upsert=True)
test = 132
# 定期更新=>每月一次


