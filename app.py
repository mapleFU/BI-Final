from common import load_elastic_search, load_graph

from flask import Flask, jsonify
from flask.views import View
from flask_cors import CORS
from flask_caching import Cache

from neo4j import GraphDatabase
from py2neo.data import Node, Relationship
from py2neo import Graph, NodeMatcher, Database
import py2neo
from elasticsearch import Elasticsearch

import os
import random
from typing import List, Dict

g = load_graph()
app = Flask(__name__)
CORS(app)
cache = Cache(app, config={
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "simple", # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 300
})


LABEL_ATTR_SET = {"institution", "title", "organizationName", "givenName", "label"}


def query_organization_name(name: str, es_client: Elasticsearch=None):
    if es_client is None:
        es_client = load_elastic_search()
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
        cur_doc = hit['_source']['doc']
        cur_doc['type'] = 'Organization'
        nodes.append(hit['_source']['doc'])
    return nodes


def tag_label(value_dict: dict):
    for k, v in value_dict.items():
        if k in LABEL_ATTR_SET:
            value_dict["label"] = value_dict[k]
            return
    print(f"Warning dict {value_dict}, No label!")


def permlize_node_result(record_node: Node):
    value_dict = dict(record_node)
    value_dict["id"] = value_dict['permID']
    labels = list(record_node.labels)
    if(labels[0]=='NewResource'):
        value_dict["type"] = labels[1]
    else:
        value_dict["type"] = labels[0]
    tag_label(value_dict)
    return value_dict


def permlize_relationship_result(record_node: Relationship):
    node_type = 'Unknown'
    try:
        node_type = list(record_node.types())[0]
    except _:
        print('Exception occured with getting node_type')
    value_dict = dict(record_node)

    value_dict["from"] = dict(record_node.start_node)['permID']
    value_dict["to"] = dict(record_node.end_node)['permID']
    value_dict["type"] = node_type
    return value_dict


def permlize_result(record: py2neo.Record):
    # print(dict(record))
    for v in dict(record).values():
        if isinstance(v, Node):
            yield (permlize_node_result(v), None)
        elif isinstance(v, Relationship):
            yield (None, permlize_relationship_result(v))


def remove_duplicate_relationships(r: List[Dict])->List[Dict]:
    # sort by from and to
    r.sort(key=lambda x: (x['from'], x['to'], x['type']))
    new_node = list()
    last_node = None
    equals = lambda a, b: a is not None and a['from'] == b['from'] \
                           and a['to'] == b['to'] and a['type'] == b['type']
    for item in r:
        if not equals(last_node, item):
            new_node.append(item)
            last_node = item
    return new_node


def remove_duplicate_nodes(r: List[Dict])->List[Dict]:
    r.sort(key=lambda x: (x['type'], x['permID']))
    new_node = list()
    last_node = None
    equals = lambda a, b: a is not None and a['permID'] == b['permID']
    for item in r:
        if not equals(last_node, item):
            new_node.append(item)
            last_node = item
    return new_node


flatten = lambda l: [item for sublist in l for item in sublist]


def merge_result(result: py2neo.Cursor):
    nodes, relations = list(), list()
    for record in result:
        print(record)
        for n, r in permlize_result(record):
            relations.append(r) if n is None else nodes.append(n)
    return {
        'nodes': remove_duplicate_nodes(nodes),
        'relationships': remove_duplicate_relationships(relations)

    }


"""
/organization/4296405163
/person/34418264994
/person/34418264994/organization/4296405163
/organization/5000716861/organization/4296405163
/person/34418264994/person/34413884412
/institution/Duke University
/industryGroup/4294952987
/businessSector/4294952745
/economicSector/4294952746
/initGraph
"""


@cache.memoize(60)
def query_organization_by_name(organization_name):
    return jsonify({
        "nodes": query_organization_name(organization_name)
    })


@app.route("/search/<org_name>")
def search_organization(org_name: str):
    return query_organization_by_name(org_name)


@app.route('/person/<pid>/organization/<oid>')
def person_organization(pid, oid):
    """
    1.输⼊入两个实体（如Alibaba和Tencent），查询其可能存在的多跳关系。其中多跳关系定义
    为，通过多条边链式的连接在⼀一起。如Alibaba -> (Industry) Internet -> Tencent
    """
    cql = f'''
        MATCH (s:Person :NewResource {{permID: '{pid}' }})-[p:isPositionIN]-(o:Organization :NewResource {{permID: '{oid}' }})
        return s, p, o
    '''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/person/<pid1>/person/<pid2>')
def person_person(pid1,pid2):
    """
    1.输⼊入两个实体（如Alibaba和Tencent），查询其可能存在的多跳关系。其中多跳关系定义
    为，通过多条边链式的连接在⼀一起。如Alibaba -> (Industry) Internet -> Tencent
    """
    cql = f'''
        MATCH (s:Person :NewResource {{permID: '{pid1}' }})-[r1]-(p)-[r2]-(o:Person :NewResource {{permID: '{pid2}' }})
        return s, r1,p,r2, o
    '''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/organization/<oid1>/organization/<oid2>')
def organization_organization(oid1, oid2):
    """
    1.输⼊入两个实体（如Alibaba和Tencent），查询其可能存在的多跳关系。其中多跳关系定义
    为，通过多条边链式的连接在⼀一起。如Alibaba -> (Industry) Internet -> Tencent
    """
    cql = f'''
        MATCH (s:Organization :NewResource {{permID: '{oid1}' }})-[r1]-(p)-[r2]-(o:Organization :NewResource {{permID: '{oid2}' }})
        return s, r1,p,r2, o
    '''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/person/<pid>')
def person(pid):
    """
    2。输⼊入⼀一个实体（如Alibaba），查询其关联的所有关系和关联实体；
    """
    cql=f'''MATCH (s:Person :NewResource {{permID:'{pid}'}})-[p]-(o) return s, p, o '''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/organization/<oid>')
def organization_show_all(oid):
    """
    #5.
    2。输⼊入⼀一个实体（如Alibaba），查询其关联的所有关系和关联实体；
    """
    cql=f'''MATCH (s :Organization :NewResource{{permID:'{oid}'}})-[p]-(o) return  s, p, o'''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/institution/<iname>')
def institution(iname):
    """
    #6.查看某个institution相关的person
    #不在要求内

    感觉很危险
    """
    cql=f'''MATCH (s :Institution {{name:'{iname}'}})-[p:fromInstitutionName]-(o:Person) return  s, p, o'''

    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/industryGroup/<iid>')
def industry_group_to_organization(iid):
    """
    #7.查看某个industryGroup相关的organization
    #不在要求内
    """
    cql=f'''MATCH (s :IndustryGroup :NewResource {{permID:'{iid}'}})-[p]-
    (o :Organization :NewResource) return  s, p, o limit 20'''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/initGraph')
def initGraph():
    """
    #8.初始化图
    """
    randomoffset=int(round(random.random()*10000,0))
    cql='''
    MATCH (s:Person)-[p]-(o:Organization) 
    RETURN s,p,o
    SKIP {} LIMIT 50 '''.format(randomoffset)

    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/economicSector/<eid>')
def economicSector(eid):
    """
    #10.
    """

    cql = f'''MATCH (s:EconomicSector{{permID:'{eid}'}})-[p]-(o:Organization) return  s, p, o limit 200'''

    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/businessSector/<eid>')
def businessSector(eid):
    """
    #11.
    """

    cql = f'''MATCH (s:BusinessSector{{permID:'{eid}'}})-[p]-(o:Organization) return  s, p, o limit 200'''

    print(cql)
    return jsonify(merge_result(g.run(cql)))


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
