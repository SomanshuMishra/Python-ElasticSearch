from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import pandas as pd

es = Elasticsearch()

# SEARCH ALL DATA IN INDEX
def all_data_search():
    s = Search(using=es, index="mexico")
    df = pd.DataFrame([hit.to_dict() for hit in s.scan()])
    print(df)
    
# all_data_search()


search_param = {
    'query': {
        'match': {
            "_id": "go_S130BPH6fduj-9dcH"
        }
    }
}  

# SEARCH ALL DATA
def search_Param():
    es = Elasticsearch()
    ress = Search(using=es,index='111_csv_file') 
    d = pd.DataFrame([hit.to_dict() for hit in ress.scan()])
    print(d)
    
# search_Param()


# SEND DATA 
data = {"Name":"Mansi Mishra","Age":17,"Address":"Panchwati Ext Same"}
def send_data(data):
    res = es.index(index='new-index',doc_type='devops',body=data)
    if(res):
        print(res)
        print('SUCCESS')
    else:
        print('FAILED')


send_data(data)

# RECEIVE PARTICULAR DATA
r_data = {
    'query': {
        'match': {
            '_id': 'qsgr2X0BfoR5egwvkgM_'
        }
    }
}
def receive_data():
    r = es.search(index="china_export_data",body=r_data)
    print(r)
    print(type(r))
    

# receive_data()