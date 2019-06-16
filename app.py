from flask import Flask, jsonify
from flask.views import View
from neo4j import GraphDatabase
from py2neo.data import Node, Relationship
from py2neo import Graph, NodeMatcher, Database
import py2neo

import os

driver_address = ''

exist = os.environ.get('is_local', None)
if exist is None:
    driver_address = 'bolt://0.tcp.ngrok.io:19185'
else:
    driver_address = "bolt://localhost:7687"

g = Graph(driver_address)


app = Flask(__name__)


LABEL_ATTR_SET =set(["institution", "title", "organizationName", "givenName"])


def tag_label(value_dict: dict):
    for k, v in value_dict.items():
        if k in LABEL_ATTR_SET:
            value_dict["label"] = value_dict[k]
            return
    print("Warning, No label!")


def permlize_node_result(record_node: Node):
    value_dict = dict(record_node)
    value_dict["id"] = value_dict['permID']
    tag_label(value_dict)
    return value_dict


def permlize_relationship_result(record_node: Relationship):
    value_dict = dict(record_node)
    value_dict["from"] = dict(record_node.start_node)['permID']
    value_dict["to"] = dict(record_node.end_node)['permID']
    return value_dict


def permlize_result(record: py2neo.Record):
    # print(dict(record))
    for v in dict(record).values():
        if isinstance(v, Node):
            yield (permlize_node_result(v), None)
        elif isinstance(v, Relationship):
            yield (None, permlize_relationship_result(v))


flatten = lambda l: [item for sublist in l for item in sublist]


def merge_result(result: py2neo.Cursor):
    nodes, relations = list(), list()
    for record in result:
        print(record)
        for n, r in permlize_result(record):
            relations.append(r) if n is None else nodes.append(n)
    return {
        'nodes': nodes,
        'relationships': relations
    }
"""
/organization/4296405163
/person/34418264994
/person/34418264994/organization/4296405163
"""


@app.route('/person/<pid>/organization/<oid>')
def person_organization(pid,oid):
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
def organization(iid):
    """
    #6.查看某个institution相关的person
    #不在要求内
    """
    cql=f'''MATCH (s:Institution{{name:'{iname}'}})-[p]-(o) return  s, p, o'''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/industryGroup/<ilabel>')
def industry_group_to_organization(ilabel):
    """
    #7.查看某个industryGroup相关的organization
    #不在要求内
    """
    cql=f'''MATCH (s:IndustryGroup{{name:'{ilabel}'}})-[p]-(o) return  s, p, o'''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


if __name__ == '__main__':
    app.run(debug=True)
