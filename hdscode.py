# code for hds dump
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
coll1=db.hds
d1=1504204200000
d2=1528828200000
cur=coll.find({"$and":[{"publishDate":{"$lte" :d2, "$gte" :d1}},{"rooms.rooms.hds":{"$exists":True}}]})
cur1=coll1.find({})
catdic={}
raredic={}
cou=0
cur2=coll.find({"$and":[{"projectId":{"$exists": True}},{"publishDate":{"$exists": True}}]})
for r in cur2:
    if r['projectId'] not in raredic:
        raredic[r['projectId']]=r['publishDate']
    else:
        if (raredic[r['projectId']]<r['publishDate']):
            raredic[r['projectId']]=r['publishDate']
for q in cur1:
    if 'category' in q:
      catdic[str(q['_id'])]=q['category']
dic={}
dicb=[]
cou=0
for x in cur :
  if x['publishDate']==raredic[x['projectId']]:
    for y in x['rooms']:
         for z in y['rooms']:
            if 'hds' in z:
                      for f in z['hds']:                     
                          if 'hdsName' in f:
                             dic['customerEmail']=x['customerEmail']
                             dic['priceVersion']=x['priceVersion']
                             dic['Date']=datetime.datetime.fromtimestamp(x['publishDate']/1000.0)
                             dic['hdsName']=f['hdsName']
                             dic['Price']=f['hdsPrice']
                             dic['ProjectId']=x['projectId']
                             dic['version']=x['version']
                             if f['hdsId'] in catdic:
                               dic['category']=catdic[f['hdsId']]
                             dicb.append(dic)
                             dic={}
                          else:
                            continue
            else:         
               continue
with open('hdsJson.json','w') as outfile:
    json.dump(dicb,outfile,default=str)
df=pd.DataFrame(dicb)
df.to_csv('hdsfinals.csv')
