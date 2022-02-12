# from elasticsearch import Elasticsearch
# es = Elasticsearch([{"host": "localhost", "port": 9200}])

# # 定義切片數目等資訊
# def get_setting():
#     settings = {
#       "index": {
#         "number_of_shards": 1, 
#         "number_of_replicas": 1,
#       }
#     }
#     return settings

# # 定義dataType
# def get_mappings():
#     mappings = {
#         "properties": {
#             "taxID": {"type": "keyword"},
#             "comapnyName": {
#               "type": "text",
#               'analyzer': "ik_max_word",
#               'search_analyzer': "ik_max_word"
#             },
#             "comapnyAddress": {"type": "keyword"},
#             "companyCapital": {"type": "keyword"},
#             "companySituation": {"type": "keyword"},
#         }
#     }
#     return mappings

# # 判斷索引companies是否存在，不存在就新增
# if not es.indices.exists(index="companies"):
#   body = dict()
#   body['settings'] = get_setting()
#   body['mappings'] = get_mappings()
#   # print(json.dumps(body)) #可以用json.dumps輸出來看格式有沒包錯
#   es.indices.create(index='companies', body=body,ignore=400)

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

host = 'search-demo-aids5lqccfumqswmvo2sfl2mjq.us-east-2.es.amazonaws.com/' # change to your Elasticsearch host name
region = 'us-east-2' # the region where you created your S3 bucket and Elasticsearch cluster
service = 'es'

bulk_file = open('music_bulk.json', 'r').read()

credentials = boto3.Session().get_credentials()
test = print(credentials)

awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

search = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = False,
    connection_class = RequestsHttpConnection
)

body = dict()
search.indices.create(index='companies', body=body,ignore=400)

print(123)
