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


def permlize_node_result(record_node: Node):
    value_dict = dict(record_node)
    value_dict["id"] = value_dict['permID']
    return value_dict


def permlize_relationship_result(record_node: Relationship):
    value_dict = dict(record_node)
    value_dict["from"] = dict(record_node.start_node)['permID']
    value_dict["to"] = dict(record_node.start_node)['permID']
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
        for n, r in permlize_result(record):
            relations.append(r) if n is None else nodes.append(n)
    return {
        'nodes': nodes,
        'relationships': relations
    }


@app.route('/person/<pid>/organization')
def person_organization(pid):
    """
    根据PERSON 返回 ORGANIZATION
    MATCH (p:Person{permID:$id1})-[r]-(i:Institution)
    return distinct p, r, i
    """
    cql = f'''
        MATCH (s:Person {{permID: '{pid}' }})-[p:isPositionIN]-(o:Organization)
        return s, p, o
    '''
    print(cql)
    return jsonify(merge_result(g.run(cql)))


@app.route('/person/<pid>/institution')
def person_institution(pid):
    raise NotImplemented()


@app.route('/organization/<pid>/organization')
def organization_organization(pid):
    raise NotImplemented()


if __name__ == '__main__':
    app.run(debug=True)
