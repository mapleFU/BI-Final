from flask import Flask, jsonify
from flask.views import View
from neo4j import GraphDatabase
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


def permlize_single_result(record_dict: dict):
    for _, v in record_dict.items():
        # print(type(v.labels))
        value_dict = dict(v)
        value_dict["id"] = value_dict['permID']
        yield value_dict


def permlize_result(record: py2neo.Record):
    drecord = dict(record)
    if len(drecord) <= 1:
        yield permlize_single_result(drecord)
    if len(drecord) == 3:
        dic2 = dict()
        for k in ('s', 'o'):
            value_dict = record[k]
            value_dict["id"] = value_dict['permID']
            dic2[k] = value_dict["id"]
            yield value_dict
        value_dict = record['p']
        value_dict["from"] = dic2['s']
        value_dict["to"] = dic2['o']
        yield value_dict


flatten = lambda l: [item for sublist in l for item in sublist]


def merge_result(result: py2neo.Cursor):
    return flatten([permlize_result(r) for r in result])


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
