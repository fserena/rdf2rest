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
from threading import Thread

import os
import time
from rdflib import RDF, URIRef
from rdflib.namespace import Namespace

PARTITION = Namespace("http://rdf2rest.org/partition:")

ns_dict = json.loads(os.environ.get('NAMESPACES', '{}'))
NAMESPACES = {k: Namespace(ns_dict[k]) for k in ns_dict.keys()}

__author__ = 'Fernando Serena'


def create_partition(source_graph, dest_graph, query, limit=None, offset=0, filename=None, ignore=None):
    def explore_linked_resource(resource):
        linked_resources = 0
        if resource not in explored_resources:
            explored_resources.add(resource)
            po = source_graph.predicate_objects(resource)
            for p, o in po:
                if p != RDF.type and isinstance(o, URIRef):
                    if ignore is None or (ignore is not None and p not in ignore):
                        any_other_subject = list(
                            source_graph.query("""ASK {?s ?p <%s> FILTER(?s!=<%s>)}""" % (o, resource))).pop()
                        if not any_other_subject or any(dest_graph.triples((o, None, None))):
                            linked_resources += 1
                            linked_resources += explore_linked_resource(o)
                dest_graph.add((resource, p, o))
        return linked_resources

    for prefix, ns in source_graph.namespaces():
        dest_graph.bind(prefix, ns)
    explored_resources = set([])

    if filename is None:
        actual_filename = 'partition'
    else:
        actual_filename = filename
    if limit is not None and isinstance(limit, int):
        query += ' LIMIT {}'.format(limit)
        if filename is None:
            actual_filename += '-{}'.format(limit)
    if offset:
        query += ' OFFSET {}'.format(offset)
        if filename is None:
            actual_filename += '-{}'.format(offset)
    qresult = source_graph.query(query)
    roots = set([r.r for r in qresult])

    partition_size = len(roots)

    for root in roots:
        dest_graph.add((root, RDF.type, PARTITION.Root))
        [dest_graph.add(t) for t in source_graph.triples((root, RDF.type, None))]
    for root in roots:
        print 'Exploring {}...'.format(root.n3()),
        linked_resources = explore_linked_resource(root)
        partition_size += linked_resources
        print ' {} more resources linked'.format(linked_resources)

    if filename is None:
        actual_filename += '.ttl'

    print 'The partition was fully created.'
    print ' - Total resources: {}'.format(partition_size)
    print 'Serializing partition graph to {}...'.format(actual_filename),
    with open(actual_filename, 'w') as f:
        f.write(dest_graph.serialize(format='turtle'))
    print 'Done.'

    return actual_filename


def create_link_partition(source_graph, dest_graph, link, file_name=None, limit=None, offset=0, ignore=None):
    if file_name is None:
        file_name = '{}_partition'.format(source_graph.namespace_manager.qname(link))
    return create_partition(source_graph, dest_graph,
                            """SELECT ?r WHERE { ?a %s ?r }""" % source_graph.namespace_manager.qname(link),
                            filename=file_name, limit=limit,
                            offset=offset,
                            ignore=ignore)


def create_type_partition(source_graph, dest_graph, ty, file_name=None, limit=None, offset=0, ignore=None):
    if file_name is None:
        file_name = '{}_partition'.format(source_graph.namespace_manager.qname(ty))
    return create_partition(source_graph, dest_graph,
                            """SELECT ?r WHERE { ?r a %s }""" % source_graph.namespace_manager.qname(ty),
                            filename=file_name, limit=limit, offset=offset,
                            ignore=ignore)


def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size


def load_dataset(g, path, filename, blocking=False):
    def load_ttl():
        with open(filename) as db:
            print '\nParsing file {}...'.format(filename),
            g.parse(file=db, format='turtle')
            print 'Done.'
        print "Dataset loaded from {} to '{}' path.".format(filename, path)
        status['loading'] = False

    def show_condition():
        size = get_size(path)
        size_changed = status['last_size'] != size
        if size_changed:
            status['last_size'] = size

        if not status['loading']:
            if size_changed:
                return True
            else:
                if status['zero_prints']:
                    status['zero_prints'] = False
                    return True
                else:
                    return False
        return True

    def show_store_size():
        def sizeof_fmt(num, suffix='B'):
            for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
                if abs(num) < 1024.0:
                    return "%3.1f%s%s" % (num, unit, suffix)
                num /= 1024.0
            return "%.1f%s%s" % (num, 'Y', suffix)

        while True:
            if show_condition():
                print "'{}' store size: {}".format(path, sizeof_fmt(float(status['last_size'])))
            else:
                break
            time.sleep(5)

    g.open(path, create=True)
    loader = Thread(target=load_ttl)
    counter = Thread(target=show_store_size)
    counter.daemon = True
    loader.daemon = True
    status = {'loading': True, 'zero_prints': True, 'after': False, 'last_size': get_size(path)}
    loader.start()
    counter.start()
    if blocking:
        loader.join()
        counter.join()
