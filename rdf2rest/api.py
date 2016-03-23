"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""
import json
import logging
import sys

import os
from flask import Flask, url_for, make_response, jsonify
from rdf2rest.dataset import PARTITION, NAMESPACES
from rdflib import ConjunctiveGraph, RDF, URIRef, Graph

__author__ = 'Fernando Serena'

URI_PREFIX = os.environ.get('URI_PREFIX', '')
SERVICE_TYPE = URIRef(os.environ.get('SERVICE_TYPE_URI', ''))
CONTAINMENT_LINK = URIRef(os.environ.get('CONTAINMENT_LINK_URI', ''))

sl_dict = json.loads(os.environ.get('SERVICE_LINKS', '{}'))
SERVICE_LINKS = {URIRef(p): sl_dict[p] for p in sl_dict.keys()}

log = logging.getLogger('rdf2rest.api')

if not SERVICE_TYPE:
    log.error('No service type is defined.')
    sys.exit(0)
if not CONTAINMENT_LINK:
    log.error('No containment link is defined.')
    sys.exit(0)

service_graph = ConjunctiveGraph('Sleepycat')

app = Flask(__name__)


def new_graph():
    g = Graph()
    for prefix in NAMESPACES:
        g.bind(prefix, NAMESPACES[prefix])
    return g


class APIError(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


class NotFound(APIError):
    def __init__(self, message, payload=None):
        super(NotFound, self).__init__(message, 404, payload)


class Conflict(APIError):
    def __init__(self, message, payload=None):
        super(Conflict, self).__init__(message, 409, payload)


@app.errorhandler(APIError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route('/<rid>')
def get_resource(rid):
    real_uri = URIRef(u'{}{}'.format(URI_PREFIX, rid))
    po = list(service_graph.predicate_objects(real_uri))
    g = new_graph()
    me = URIRef(url_for('get_resource', rid=rid, _external=True))
    if not po:
        raise NotFound("Resource {} not found".format(rid))
    for p, o in po:
        if o == PARTITION.Root:
            continue
        if isinstance(o, URIRef):
            if list(service_graph.objects(o, RDF.type)):
                rid = unicode(o).replace(URI_PREFIX, "")
                o = URIRef(url_for('get_resource', rid=rid, _external=True))

        if p in SERVICE_LINKS:
            rid = unicode(o).replace(URI_PREFIX, "")
            o = URIRef('{}{}'.format(SERVICE_LINKS[p], rid))

        g.add((me, p, o))
    response = make_response(g.serialize(format='turtle'))
    response.headers['Content-Type'] = 'text/turtle'
    return response


@app.route('/')
def get_service():
    g = new_graph()
    me = URIRef(url_for('get_service', _external=True))
    g.add((me, RDF.type, SERVICE_TYPE))
    for db_resource in service_graph.subjects(RDF.type, PARTITION.Root):
        db_resource = URIRef(
            url_for('get_resource', rid=db_resource.replace(URI_PREFIX, ""), _external=True))
        g.add((me, CONTAINMENT_LINK, db_resource))
    response = make_response(g.serialize(format='turtle'))
    response.headers['Content-Type'] = 'text/turtle'
    return response
