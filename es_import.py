from py2neo import Graph, Record
from elasticsearch import Elasticsearch, helpers

from common import load_graph, load_elastic_search

from typing import Dict


def build_json(result: Record)->Dict:
    organization_dict = dict(dict(result)['o'])
    if 'organizationName' in organization_dict:
        return {
            "_index": "organizations",
            "_type": "doc",
            "doc": {
                "organizationName": organization_dict['organizationName'],
                'permID': organization_dict['permID']
            }
        }
    else:
        print(organization_dict.keys())
    raise ValueError("organizationName not exists")


def query_organization_name(es_client: Elasticsearch, name: str):
    es_query = {
        "query": {
            "match": {
                "doc.organizationName": name
            }

        }
    }
    res = es_client.search(index="organizations", body=es_query)
    print("Got %d Hits:" % res['hits']['total']['value'])
    nodes = []
    for hit in res['hits']['hits']:
        nodes.append(hit['_source']['doc']))
    return nodes


if __name__ == '__main__':
    g: Graph = load_graph()
    es = load_elastic_search()
    query_organization_name(es, 'alibaba')
    # cnt = 0
    # batch_size = 9000
    # bulk = []
    # for r in g.run("MATCH (o:Organization) RETURN o"):
    #     bulk.append(build_json(r))
    #     cnt += 1
    #     if cnt % batch_size == 0:
    #         print(f"data up to {cnt}")
    #         helpers.bulk(es, bulk)
    #         print(f"bulk write done")
    #         bulk.clear()
    # helpers.bulk(es, bulk)
    # print('bulk finish')