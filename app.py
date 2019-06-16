from common import load_elastic_search
from flask import Flask, jsonify
from flask.views import View
from flask_cors import CORS

from neo4j import GraphDatabase
from py2neo.data import Node, Relationship
from py2neo import Graph, NodeMatcher, Database
import py2neo

import os
from typing import List, Dict

driver_address = ''

exist = os.environ.get('is_local', None)
if exist is None:
    driver_address = 'bolt://0.tcp.ngrok.io:19185'
else:
    driver_address = "bolt://localhost:7687"

g = Graph(driver_address)


app = Flask(__name__)
CORS(app)


LABEL_ATTR_SET =set(["institution", "title", "organizationName", "givenName", "label"])


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
        # print(record)
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
/organization/5043331619/organization/4296405163
/person/34418264994/person/34413884412
/institution/Duke University
/industryGroup/4294952987
"""


@app.route('/person/<pid>/organization/<oid>')
def person_organization(pid, oid):
    """
    1.输⼊入两个实体（如Alibaba和Tencent），查询其可能存在的多跳关系。其中多跳关系定义
    为，通过多条边链式的连接在⼀一起。如Alibaba -> (Industry) Internet -> Tencent
    """
    cql = f'''
        MATCH (s:Person {{permID: '{pid}' }})-[p:isPositionIN]-(o:Organization{{permID: '{oid}' }})
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
        MATCH (s:Person {{permID: '{pid1}' }})-[p]-(o:Person{{permID: '{pid2}' }})
        return s, p, o
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
        MATCH (s:Organization {{permID: '{oid1}' }})-[p]-(o:Organization{{permID: '{oid2}' }})
        return s, p, o
    '''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/person/<pid>')
def person(pid):
    """
    2。输⼊入⼀一个实体（如Alibaba），查询其关联的所有关系和关联实体；
    """
    cql=f'''MATCH (s:Person{{permID:'{pid}'}})-[p]-(o) return s, p, o '''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/organization/<oid>')
def organization_show_all(oid):
    """
    #5.
    2。输⼊入⼀一个实体（如Alibaba），查询其关联的所有关系和关联实体；
    """
    cql=f'''MATCH (s:Organization{{permID:'{oid}'}})-[p]-(o) return  s, p, o'''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/institution/<iname>')
def institution(iid):
    """
    #6.查看某个institution相关的person
    #不在要求内
    """
    cql=f'''MATCH (s:Institution{{name:'{iname}'}})-[p]-(o) return  s, p, o'''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/industryGroup/<iid>')
def industry_group_to_organization(iid):
    """
    #7.查看某个industryGroup相关的organization
    #不在要求内
    """
    cql=f'''MATCH (s:IndustryGroup{{permID:'{iid}'}})-[p]-(o:Organization) return  s, p, o limit 20'''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


if __name__ == '__main__':
    app.run(debug=True)
