# code for furniture dump
import pymongo
import pandas as pd
import pprint
import json
import csv
import datetime
from bson.json_util import loads
client=pymongo.MongoClient()
db=client["dev-iqdb"]
coll=db.quotedatas
d1=1504204200000
d2=1528828200000
raredic={}
cur2=coll.find({"$and":[{"projectId":{"$exists": True}},{"publishDate":{"$exists": True}}]})
for r in cur2:
    if r['projectId'] not in raredic:
        raredic[r['projectId']]=r['publishDate']
    else:
        if (raredic[r['projectId']]<r['publishDate']):
            raredic[r['projectId']]=r['publishDate']
cur=coll.find({"$and":[{"publishDate":{"$gte" :d1, "$lte" :d2}},{"rooms.rooms.ulProducts":{"$exists":True}}]})
dic={}
dicb=[]
for x in cur :
  if x['publishDate']==raredic[x['projectId']]:
    for y in x['rooms']:
         for z in y['rooms']:
            if 'ulProducts' in z:
                      for f in z['ulProducts']:
                          if 'price' in f:
                             dic['customerEmail']=x['customerEmail']
                             dic['priceVersion']=x['priceVersion']
                             dic['Date']=datetime.datetime.fromtimestamp(x['publishDate']/1000.0)
                             dic['price']=f['price']
                             dic['ProjectId']=x['projectId']
                             dic['version']=x['version']
                             if 'name' in f:
                                 dic['name']=f['name']
                             else:
                                 dic['name']=None
                             if 'sku' in f:
                                dic['SKU']=f['sku']
                             else:
                                dic['SKU']=None
                             dicb.append(dic)
                             dic={}
                          else:
                            continue
            else:
                continue
with open('furnitureJson.json','w') as outfile:
    json.dump(dicb,outfile,default=str)
df=pd.DataFrame(dicb)
df.to_csv('furniturefinal.csv')
