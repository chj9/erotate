#!/bin/python
# coding=utf-8
import re
import sys
import time
import json
from functools import reduce

import click
import fnmatch
import logging
import urllib3
import itertools
from math import ceil
from tabulate import tabulate
from datetime import datetime
from collections import OrderedDict
from elasticsearch.helpers import scan
from elasticsearch import Elasticsearch, exceptions
from distutils.version import StrictVersion
from humanfriendly import parse_size, format_size

""" external libraries
- click
- urllib3
- tabulate
- elasticsearch
- humanfriendly
"""

""" TODO
- fake_index_by_name support timezone
- generate kibana index pattern
- add time field
- preserved date support: --preserved_date '2019.12.01~2019.12.10,2020.01.01~2020.01.10'
- inspect by group
- monitoring inspect storage per day
- alias management ? ( or dedicated ealias script ? )
- auto detect shards nodes ratio
- lifecycle better time 
"""


REASON_PEER_SHARD_ON_NODE = 'the shard cannot be allocated to the same node on which a copy of the shard already exists'
REASON_TOTAL_SHARDS_PER_NODE = 'index.routing.allocation.total_shards_per_node'

rollover_index = '.rollover_indices'
hot_warm_attribute = 'box_type'
VERSION = '2.0.2'
META_ID = '__meta__'

now = datetime.now()
now_timestamp = int(time.time())
es = None
metainfo = {}
cluster_info = {}
health = {}
nodes = {}
cluster_settings = {}
routing_rule = {}
migration = {
    '2.0.0': [{'set': {'field': 'timezone', 'value': '+08:00'}}],
    '2.0.1': [{'set': {'field': 'time_field', 'value': '@timestamp'}}],
}


def chunks(iterable, size=1000):
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size-1))


def chunks_list(iterable, size):
    chunk_list = []
    for chunk in chunks(iterable, size):
        chunk_list.append([i for i in chunk])
    return chunk_list


def uniq(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def flatten(d):
    def items():
        for key, value in d.items():
            if isinstance(value, dict):
                for subkey, subvalue in flatten(value).items():
                    yield key + "." + subkey, subvalue
            else:
                yield key, value
    return dict(items())


def deep_get(dct, *keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


def make_pattern_string(name, pattern, timezone, offset=None):
    return '<%s-{now/d{%s|%s}}-%s>' % (name, pattern, timezone, str(offset) if offset else '*') if pattern else '%s-%s' % (name, str(offset) if offset else '*')


def make_rollover_index_by_version():
    return '%s-%s' % (rollover_index, VERSION)


def make_pipeline_name(function):
    return '%s-%s' % (rollover_index, function)


def make_hot_name(name):
    return '%s.hot' % name


def make_hot_name_pattern():
    return '*.hot'


def fnmatch_list(key, patterns):
    for p in patterns:
        if fnmatch.fnmatch(key, p):
            return True
    return False


def create_rollover_index(with_alias):
    mapping = {'dynamic_templates': [{'rollover_strings': {'mapping': {'type': 'keyword'}, 'match_mapping_type': 'string'}}]}
    es.indices.create(make_rollover_index_by_version(), body={
        'settings': {'index': {'refresh_interval': '1s', 'number_of_shards': 1}},
        'mappings': {'_default_': mapping} if StrictVersion(cluster_info['version']['number']) < StrictVersion('7.0.0') else mapping,
        'aliases': {rollover_index: {}} if with_alias else {},
    })
    global metainfo
    metainfo = {'version': VERSION}
    es.index(index=make_rollover_index_by_version(), body=metainfo, id=META_ID, doc_type='_doc')


# TODO timezone support
def fake_index_by_name(name, pattern, timezone='+08:00'):
    """
        generate a fake index name for index template matching
        ATTENTION:
        - rollover postfix is not supported in index template pattern
        - timezone is not supported cause tz is not well supported in python 2.x
    """
    if pattern == 'YYYY.MM.dd':
        return '%s-%s-000001' % (name, now.strftime('%Y.%m.%d'))
    elif pattern == 'YYYY.MM':
        return '%s-%s-000001' % (name, now.strftime('%Y.%m'))
    elif pattern == 'YYYY':
        return '%s-%s-000001' % (name, now.strftime('%Y.%m'))
    else:
        return '%s-000001' % name


def filtered_data_nodes():
    data_nodes = {}
    for node_id, node in nodes.items():
        if 'data' in node['roles']:
            data_nodes[node_id] = node
    return data_nodes


def extract_new_create_index_node_type(policy_list):
    return ['hot', 'warm', 'cold'][:len(policy_list)][map(lambda x: x > 0, policy_list).index(True)]


def extract_policy_string(policy_string):
    if not policy_string:
        return []
    policy_list = map(int, policy_string.split(','))
    if len(policy_list) > 3:
        raise Exception('policy string syntax error')
    return policy_list


def get_day_start_timestamp(timestamp):
    return time.mktime(datetime.fromtimestamp(timestamp).replace(hour=0, minute=0, second=0, microsecond=0).timetuple())


def get_month_start_timestamp(timestamp):
    return time.mktime(datetime.fromtimestamp(timestamp).replace(day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())


def get_year_start_timestamp(timestamp):
    return time.mktime(datetime.fromtimestamp(timestamp).replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).timetuple())


def get_metadata(name, meta=False):
    metadata = es.get(index=rollover_index, id=name, doc_type='_doc')
    return metadata if meta else metadata['_source']


def get_metadatas(name):
    names = set()
    for n in name.split(','):
        for item in scan(es, index=rollover_index, query={'query': {'wildcard': {'name': {'value': n}}}}, scroll='10m'):
            if item['_id'] == META_ID or item['_source']['name'] in names:
                continue
            names.add(item['_source']['name'])
            yield item['_source']


def get_old_writing_index(name):
    for k, v in es.indices.get_alias(name=name).items():
        if v['aliases'][name].get('is_write_index', False):
            return k


def get_total_shards_per_node(number_of_shards, number_of_replicas, number_of_nodes):
    return int(ceil(float(number_of_shards) * (1 + float(number_of_replicas)) / number_of_nodes))


def get_number_of_shards(shards_nodes_ratio, number_of_nodes):
    return max(int(ceil(number_of_nodes * float(shards_nodes_ratio) / 100)), 1)


def get_nodes_by_allocation(allocation):
    """ get routing-available nodes by allocation filtering """
    nodes_require = set()
    nodes_exclude = set()
    nodes_include = {}
    for node_id, node in filtered_data_nodes().items():
        node_attr = node['attributes']
        node_attr.update({'_ip': node['ip'], '_host': node['host'], '_name': node['name']})
        for need, need_attr in allocation.items():
            for key, need_value_string in flatten(need_attr).items():
                node_values = node_attr.get(key)
                if node_values:
                    if not isinstance(node_values, list):
                        node_values = [node_values]
                    if need == 'require':
                        if len(need_value_string.split(',')) > 1:
                            continue
                        for node_value in node_values:
                            if fnmatch.fnmatch(node_value, need_value_string):
                                nodes_require.add(node_id)
                                break
                    elif need == 'include':
                        for need_value in need_value_string.split(','):
                            for node_value in node_values:
                                if fnmatch.fnmatch(node_value, need_value):
                                    nodes_include.setdefault(key, set())
                                    nodes_include[key].add(node_id)
                                    break
                            else:
                                continue
                            break
                    elif need == 'exclude':
                        for need_value in need_value_string.split(','):
                            for node_value in node_values:
                                if fnmatch.fnmatch(node_value, need_value):
                                    nodes_exclude.add(node_id)
                                    break
                            else:
                                continue
                            break
    return [nodes_require, nodes_include.values(), nodes_exclude]


def get_index_allocation(index, node_type=None):
    """ get allocation filtering rule from index template """
    allocation = {}
    assigned_rule = {}
    if node_type:
        allocation = {'require': {hot_warm_attribute: node_type}}
    for rule_name, rule in routing_rule.items():
        for pattern in rule['patterns']:
            if fnmatch.fnmatch(index, pattern):
                break
        else:
            continue
        for need, need_attr in rule['allocation'].items():
            for key, need_value_string in need_attr.items():
                id = '%s.%s' % (need, key)
                if deep_get(allocation, need, key):
                    original_rule = assigned_rule.get(id, {})
                    if node_type is not None and need == 'require' and key == hot_warm_attribute:
                        continue
                    elif original_rule.get('order') == rule['order']:
                        raise Exception('templates with same allocation has same order: [%s.%s] %s %s', need, key, rule_name, original_rule.get('name'))
                    elif original_rule.get('order') > rule['order']:
                        continue
                allocation.setdefault(need, {})
                allocation[need][key] = need_value_string
                assigned_rule[id] = rule
    return allocation


def get_number_of_nodes(index, node_type=None):
    return get_number_of_nodes_by_allocation(get_index_allocation(index, node_type))


def get_number_of_nodes_by_allocation(index_allocation):
    """ get number of nodes by hot-warm node_type、
    cluster level allocation filtering、
    index template level allocation filtering
    """
    data_nodes_all = set(filtered_data_nodes().keys())
    cluster_allocation = deep_get(cluster_settings['persistent'], 'cluster', 'routing', 'allocation') or {}
    cluster_allocation.update(deep_get(cluster_settings['transient'], 'cluster', 'routing', 'allocation') or {})
    cluster_allocation = dict((k, cluster_allocation[k]) for k in cluster_allocation if k in ['require', 'include', 'exclude'])
    index_allocation = dict((k, index_allocation[k]) for k in index_allocation if k in ['require', 'include', 'exclude'])
    logging.info('index allocation: %s' % index_allocation)
    [cluster_nodes_require, cluster_nodes_include, cluster_nodes_exclude] = get_nodes_by_allocation(cluster_allocation)
    [index_nodes_require, index_nodes_include, index_nodes_exclude] = get_nodes_by_allocation(index_allocation)
    cluster_nodes = reduce(lambda x, y: x & y, cluster_nodes_include or [data_nodes_all], cluster_nodes_require or data_nodes_all) - cluster_nodes_exclude
    index_nodes = reduce(lambda x, y: x & y, index_nodes_include or [data_nodes_all], index_nodes_require or data_nodes_all) - index_nodes_exclude
    number_of_nodes = len(cluster_nodes & index_nodes)
    if number_of_nodes == 0:
        click.echo('index allocation: %s' % index_allocation, err=True)
        click.echo('cluster allocation: %s' % cluster_allocation, err=True)
        raise Exception('can not get number of nodes, please check your allocation settings above')
    logging.info('number of nodes: %d' % number_of_nodes)
    return number_of_nodes


def get_number_of_nodes_by_name(name, pattern, node_type=None):
    return get_number_of_nodes(fake_index_by_name(name, pattern), node_type)


def get_number_of_nodes_by_policy(name, pattern, policy_list):
    if not policy_list or len(policy_list) == 1:
        node_type = None
    else:
        node_type = extract_new_create_index_node_type(policy_list)
    return get_number_of_nodes_by_name(name, pattern, node_type)


def get_indices_start_end_timestamp(indices, time_field):
    search_list = []
    for index in indices:
        search_list.append({'index': index})
        search_list.append({'size': 0, 'aggs': {'max': {'max': {'field': time_field}}, 'min': {'min': {'field': time_field}}}})
    indices_start_end_dict = {}
    for i, resp in enumerate(es.msearch(reduce(lambda x, y: x + '%s \n' % json.dumps(y), search_list, ''))['responses']):
        start_timestamp = (resp['aggregations']['min']['value'] or 0) / 1000
        end_timestamp = (resp['aggregations']['max']['value'] or 0) / 1000
        duration_seconds = end_timestamp - start_timestamp
        indices_start_end_dict[indices[i]] = {'start': start_timestamp, 'end': end_timestamp, 'duration': duration_seconds}
    return indices_start_end_dict


def erotate_upgrade_migrate(from_version, from_index):
    if StrictVersion(from_version) > StrictVersion(VERSION):
        raise Exception('higher version metadata in rollover_index, please upgrade your script')
    elif from_version == VERSION:
        return
    logging.warning('should migrate from %s to %s', from_version, VERSION)
    pipeline = None
    processors = []
    for ver, proc in migration.items():
        if StrictVersion(from_version) < StrictVersion(ver):
            processors += proc
    reindex_body = {
        'source': {'index': from_index, 'query': {'bool': {'must_not': {'term': {'_id': META_ID}}}}},
        'dest': {'index': make_rollover_index_by_version()}
    }
    if processors:
        pipeline = make_pipeline_name('migrate-from-%s-to-%s' % (from_version, VERSION))
        es.ingest.put_pipeline(pipeline, body={'processors': processors})
        reindex_body['dest']['pipeline'] = pipeline
    logging.warning('creating new index....')
    create_rollover_index(False)
    logging.warning('reindexing from %s to %s with pipeline-id: %s', from_index, make_rollover_index_by_version(), pipeline)
    es.reindex(body=reindex_body, refresh=True)
    logging.warning('deleting old index...')
    es.indices.delete(from_index)
    logging.warning('adding alias to new index...')
    es.indices.put_alias(make_rollover_index_by_version(), rollover_index)


@click.group()
@click.option('--hosts', default='localhost:9200', help='hosts split by comma', show_default=True)
@click.option('--http_auth', help='http auth string')
@click.option('--ssl', default=False, help='use ssl', is_flag=True)
@click.option('--level', default='WARN', help='log level')
@click.option('--hot_warm_attr', default='box_type', help='hot warm attribute', show_default=True)
def cli(hosts, http_auth, ssl, level, hot_warm_attr):
    args = {}
    if http_auth:
        args['http_auth'] = http_auth
    if ssl:
        args['use_ssl'] = True
        args['verify_certs'] = False
    logging.basicConfig(stream=sys.stdout, level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    global es, cluster_info, health, nodes, metainfo, cluster_settings, routing_rule, hot_warm_attribute
    hot_warm_attribute = hot_warm_attr
    es = Elasticsearch(hosts.split(','), timeout=300, **args)
    cluster_info = es.info()
    health = es.cluster.health()
    cluster_settings = es.cluster.get_settings()
    nodes = es.nodes.info(format='json', filter_path='nodes.*.name,nodes.*.host,nodes.*.ip,nodes.*.roles,nodes.*.attributes')['nodes']
    for name, template in es.indices.get_template().items():
        allocation = deep_get(template['settings'], 'index', 'routing', 'allocation') or {}
        if allocation:
            routing_rule[name] = {
                'patterns': template['index_patterns'],
                'order': template['order'],
                'allocation': dict((k, allocation[k]) for k in allocation if k in ['require', 'include', 'exclude'])
            }
    try:
        metainfo = get_metadata(META_ID, True)
    except exceptions.NotFoundError:
        create_rollover_index(True)
    else:
        erotate_upgrade_migrate(metainfo['_source']['version'], metainfo['_index'])


@cli.command()
@click.option('--columns', default=None)
def ls(columns):
    """ list names managed by erotate """
    rows = []
    headers = ['name', 'pattern', 'timezone', 'time_field', 'shards_nodes_ratio', 'number_of_replicas', 'rollover_size', 'policy']
    if columns:
        headers = uniq(['name'] + columns.split(','))
    for metadata in get_metadatas('*'):
        rows.append(OrderedDict([(key, metadata[key]) for key in headers]))
    click.echo(tabulate(rows, headers='keys'))


@cli.command()
@click.argument('name')
def rm(name):
    """ remove a name managed by erotate """
    es.delete(index=rollover_index, id=name, doc_type='_doc')
    click.echo('%s deleted' % name)


@cli.command('set')
@click.argument('name')
@click.option('--shards_nodes_ratio', default=0, help='the number part of pri-shards / nodes percentage, min pri-shard is 1')
@click.option('--number_of_replicas', default=1, show_default=True)
@click.option('--rollover_size', default='30g', show_default=True, help='rollover size per shard')
@click.option('--policy', default=None, help='hot-days,warm-days,cold-days or keep-days, exp.: 3,4,7 data will be on hot for 3 days, on warm for 4 days, on cold for 7 days, deleted after 14 days.')
@click.option('--pattern', default='YYYY.MM.dd', show_default=True)
@click.option('--timezone', default='+08:00', show_default=True)
@click.option('--time_field', default='@timestamp', show_default=True, help='time field for inspect')
@click.option('--force-rollover', is_flag=True)
def set_name(name, shards_nodes_ratio, number_of_replicas, rollover_size, policy, pattern, timezone, time_field, force_rollover):
    """ set a name managed by erotate """
    for n in name.split(','):
        if n == META_ID:
            raise Exception('this name is reserved')
        policy_list = extract_policy_string(policy)
        number_of_nodes = get_number_of_nodes_by_policy(n, pattern, policy_list)
        number_of_shards = get_number_of_shards(shards_nodes_ratio, number_of_nodes)
        total_shards_per_node = get_total_shards_per_node(number_of_shards, number_of_replicas, number_of_nodes)
        logging.info('total_shards_per_node: %d', total_shards_per_node)
        old_writing_index = None
        old_max_index = None
        old_max_num = 0
        try:
            old_writing_index = get_old_writing_index(n)
        except exceptions.NotFoundError:
            pass
        try:
            for v in es.cat.indices(make_pattern_string(n, pattern, timezone), format='json'):
                if pattern == 'YYYY.MM.dd':
                    regex = r'%s-\d{4}.\d{2}.\d{2}-(?P<number>\d{6}$)' % n
                elif pattern == 'YYYY.MM':
                    regex = r'%s-\d{4}.\d{2}-(?P<number>\d{6}$)' % n
                elif pattern == 'YYYY':
                    regex = r'%s-\d{4}-(?P<number>\d{6}$)' % n
                else:
                    regex = r'%s-(?P<number>\d{6}$)' % n
                match = re.match(regex, v['index'])
                if match:
                    if old_max_num < int(match.group('number')):
                        old_max_num = int(match.group('number'))
                        old_max_index = v['index']
        except exceptions.NotFoundError:
            pass
        need_force_rollover = force_rollover or not old_writing_index or old_writing_index != old_max_index
        if need_force_rollover:
            create_index_body = {'settings': {
                'index.number_of_shards': number_of_shards,
                'index.number_of_replicas': number_of_replicas,
                'index.routing.allocation.total_shards_per_node': total_shards_per_node,
            }}
            if len(policy_list) > 1:
                new_create_index_node_type = extract_new_create_index_node_type(policy_list)
                create_index_body['settings']['index.routing.allocation.require.%s' % hot_warm_attribute] = new_create_index_node_type
                if new_create_index_node_type == 'hot':
                    create_index_body['aliases'] = {make_hot_name(name): {}}
            new_index = es.indices.create(make_pattern_string(n, pattern, timezone, '{0:0>6}'.format(old_max_num + 1)), create_index_body)['index']
            click.echo('[%s] create new index: %s' % (n, new_index))
            actions = [{'add': {'index': new_index, 'alias': n, 'is_write_index': True}}]
            if old_writing_index:
                click.echo('[%s] remove write alias from: %s' % (n, old_writing_index))
                actions.insert(0, {'add': {'index': old_writing_index, 'alias': n, 'is_write_index': False}})
            click.echo('[%s] add write alias to: %s' % (n, new_index))
            es.transport.perform_request('POST', '/_aliases', body={'actions': actions})
        click.echo('[%s] save metadata to: %s' % (n, rollover_index))
        retries = 0
        while True:
            try:
                es.index(
                    index=rollover_index, body={
                        'pattern': pattern,
                        'name': n,
                        'shards_nodes_ratio': shards_nodes_ratio,
                        'number_of_replicas': number_of_replicas,
                        'rollover_size': rollover_size,
                        'timezone': timezone,
                        'time_field': time_field,
                        'policy': policy_list,
                    }, doc_type='_doc', id=n
                )
            except exceptions.TransportError as e:
                if e.status_code != 429 or retries == 3:
                    raise e
                retries += 1
                time.sleep(2 ** retries)
            else:
                break


@cli.command()
@click.argument('name')
@click.option('--shards_nodes_ratio', default=None)
@click.option('--number_of_replicas', default=None)
@click.option('--rollover_size', default=None)
@click.option('--policy', default=None)
@click.option('--pattern', default=None)
@click.option('--timezone', default=None)
@click.option('--time_field', default=None)
@click.option('--force-rollover', is_flag=True)
@click.pass_context
def update(ctx, name, shards_nodes_ratio, number_of_replicas, rollover_size, policy, pattern, timezone, time_field, force_rollover):
    """ update only specified fields """
    for metadata in get_metadatas(name):
        ctx.invoke(set_name, name=metadata['name'],
                   shards_nodes_ratio=shards_nodes_ratio if shards_nodes_ratio is not None else metadata['shards_nodes_ratio'],
                   pattern=pattern if pattern is not None else metadata['pattern'],
                   rollover_size=rollover_size if rollover_size is not None else metadata['rollover_size'],
                   number_of_replicas=number_of_replicas if number_of_replicas is not None else metadata['number_of_replicas'],
                   policy=policy if policy is not None else ','.join(map(str, metadata['policy'])),
                   force_rollover=force_rollover,
                   timezone=timezone if timezone is not None else metadata['timezone'],
                   time_field=time_field if time_field is not None else metadata['time_field']
        )


@cli.command()
@click.option('--name', default=None)
@click.option('--shards_nodes_ratio', default=None)
@click.option('--number_of_replicas', default=None)
@click.option('--force', is_flag=True)
@click.option('--dry-run', is_flag=True)
def rollover(name, shards_nodes_ratio, number_of_replicas, force, dry_run):
    """ the script rollovers indices stored in elasticsearch once \n
     you should call this function periodically at most 1 time per minute
     if the writing index is red OR border time is reached, rollover will be force executed
    """
    items = []
    names = []
    red_names = []
    for metadata in get_metadatas(name or '*'):
        items.append(metadata)
        names.append(metadata['name'])
    if health['status'] == 'red' and health['initializing_shards'] == 0:
        red_indices = []
        for v in es.cat.indices(index=','.join(names), health='red', format='json'):
            red_indices.append(v['index'])
        if red_indices:
            for red_indices_chunk in chunks(red_indices, 50):
                for k, v in es.indices.get_alias(index=','.join(red_indices_chunk)).items():
                    for name, detail in v['aliases'].items():
                        if name in names and detail.get('is_write_index', False):
                            red_names.append(name)
    is_begin_of_day = now.strftime('%H:%M') == '00:00'
    is_begin_of_month = is_begin_of_day and now.strftime('%d') == '01'
    is_begin_of_year = is_begin_of_day and is_begin_of_month and now.strftime('%m') == '01'
    for metadata in items:
        number_of_nodes = get_number_of_nodes_by_policy(metadata['name'], metadata['pattern'], metadata['policy'])
        number_of_shards = get_number_of_shards(shards_nodes_ratio or metadata['shards_nodes_ratio'], number_of_nodes)
        match = re.match(r'(?P<number>\d+)(?P<unit>.*)', metadata['rollover_size'])
        rollover_body = {
            'conditions': {'max_size': str(int(match.group('number')) * number_of_shards) + match.group('unit')},
            'settings': {
                'index.number_of_shards': number_of_shards,
                'index.number_of_replicas': number_of_replicas or metadata['number_of_replicas'],
                'index.routing.allocation.total_shards_per_node': get_total_shards_per_node(number_of_shards, number_of_replicas or metadata['number_of_replicas'], number_of_nodes)
            }
        }
        if force or \
                metadata['name'] in red_names or \
                (metadata['pattern'] == 'YYYY.MM.dd' and is_begin_of_day) or \
                (metadata['pattern'] == 'YYYY.MM' and is_begin_of_month) or \
                (metadata['pattern'] == 'YYYY' and is_begin_of_year):
            del rollover_body['conditions']
        else:
            exact_now_timestamp = int(time.time())
            if metadata['pattern'] == 'YYYY.MM.dd':
                rollover_body['conditions']['max_age'] = '%ds' % (exact_now_timestamp - get_day_start_timestamp(exact_now_timestamp) + 30)
            elif metadata['pattern'] == 'YYYY.MM':
                rollover_body['conditions']['max_age'] = '%ds' % (exact_now_timestamp - get_month_start_timestamp(exact_now_timestamp) + 30)
            elif metadata['pattern'] == 'YYYY':
                rollover_body['conditions']['max_age'] = '%ds' % (exact_now_timestamp - get_year_start_timestamp(exact_now_timestamp) + 30)

        if len(metadata['policy']) > 1:
            new_create_index_node_type = extract_new_create_index_node_type(metadata['policy'])
            rollover_body['settings']['index.routing.allocation.require.%s' % hot_warm_attribute] = new_create_index_node_type
            if new_create_index_node_type == 'hot':
                rollover_body['aliases'] = {make_hot_name(metadata['name']): {}}
        try:
            rt = es.indices.rollover(metadata['name'], body=rollover_body, params={'dry_run': 'true' if dry_run else 'false'})
            if rt['rolled_over']:
                click.echo('[%s]%s rollovered to %s' % (datetime.now().strftime('%Y-%m-%d %H:%M'), rt['old_index'], rt['new_index']))
        except exceptions.RequestError as e:
            if e.error == 'resource_already_exists_exception':
                click.echo('[%s] %s' % (datetime.now().strftime('%Y-%m-%d %H:%M'), e), err=True)
                click.echo('[%s] add write alias to: %s' % (metadata['name'], e.info['error']['index']))
                actions = [
                    {'add': {'index': get_old_writing_index(metadata['name']), 'alias': metadata['name'], 'is_write_index': False}},
                    {'add': {'index': e.info['error']['index'], 'alias': metadata['name'], 'is_write_index': True}}
                ]
                es.transport.perform_request('POST', '/_aliases', body={'actions': actions})
        except Exception as e:
            click.echo('[%s] %s' % (datetime.now().strftime('%Y-%m-%d %H:%M'), e), err=True)


@cli.command('keep-green')
@click.option('--max_relocate_size', default='500mb')
@click.option('--dry-run', is_flag=True)
def keep_green(max_relocate_size, dry_run):
    """ tries to keep the cluster green if there's an allocation issue
    """
    if health['status'] == 'green':
        logging.info('cluster is green... skip auto fix')
        return
    if health['initializing_shards'] > 0:
        logging.warning('cluster is recovering... skip auto fix')
        return
    problem_indices = es.cat.indices(health='yellow', format='json') + es.cat.indices(health='red', format='json')
    indice_settings = es.indices.get_settings(','.join([i['index'] for i in problem_indices]), 'index.routing.allocation.*')
    for i in problem_indices:
        reason_throttled = False
        reason_total_shards_per_node = False
        reason_same_shard_on_node = False
        old_total_shards_per_node = None
        relocate_min_shard_number = None
        relocate_min_shard_node = None
        relocate_dest_node = None
        relocate_min_shard_size = 0
        for s in es.cat.shards(index=i['index'], format='json', s='state'):
            if s['state'] == 'UNASSIGNED':
                explain = es.cluster.allocation_explain(body={
                    'index': s['index'],
                    'shard': s['shard'],
                    'primary': s['prirep'] == 'p'
                })
                if 'error' in explain or explain['can_allocate'] == 'throttled':
                    reason_throttled = True
                    break
                for node in explain['node_allocation_decisions']:
                    if 'deciders' in node and len(node['deciders']) == 1:
                        if REASON_TOTAL_SHARDS_PER_NODE in node['deciders'][0]['explanation']:
                            reason_total_shards_per_node = True
                            if old_total_shards_per_node is None:
                                match = re.match(r'.*index\.routing\.allocation\.total_shards_per_node=(?P<number>\d).*', node['deciders'][0]['explanation'])
                                if match:
                                    old_total_shards_per_node = int(match.group('number'))
                        elif REASON_PEER_SHARD_ON_NODE in node['deciders'][0]['explanation']:
                            reason_same_shard_on_node = True
                            relocate_dest_node = node['node_name']
            elif s['prirep'] == 'r':
                shard_size = parse_size(s['store'], True)
                if s['node'] != relocate_dest_node and (relocate_min_shard_number is None or relocate_min_shard_size > shard_size):
                    relocate_min_shard_size = shard_size
                    relocate_min_shard_number = s['shard']
                    relocate_min_shard_node = s['node']
        if reason_throttled:
            continue
        if reason_total_shards_per_node and reason_same_shard_on_node and relocate_min_shard_size <= parse_size(max_relocate_size, True):
            click.echo('[%s][%s] relocate shard %s from %s to %s' % (
                datetime.now().strftime('%Y-%m-%d %H:%M'), i['index'], relocate_min_shard_number,
                relocate_min_shard_node,
                relocate_dest_node))
            if not dry_run:
                es.cluster.reroute({'commands': [{'move': {'index': i['index'], 'shard': relocate_min_shard_number, 'from_node': relocate_min_shard_node, 'to_node':relocate_dest_node}}]})
        elif reason_total_shards_per_node:
            new_total_shards_per_node = get_total_shards_per_node(i['pri'], i['rep'],
                get_number_of_nodes_by_allocation(deep_get(indice_settings[i['index']], 'settings', 'index', 'routing', 'allocation') or {})
            )
            if new_total_shards_per_node <= old_total_shards_per_node:
                logging.warning('calculated total shards per node equals the old setting: %s', i['index'])
                new_total_shards_per_node += 1
            click.echo('[%s][%s] extend total_shards_per_node to %d' % (datetime.now().strftime('%Y-%m-%d %H:%M'), i['index'], new_total_shards_per_node))
            if not dry_run:
                es.indices.put_settings(index=i['index'], body={'index.routing.allocation.total_shards_per_node': new_total_shards_per_node})


@cli.command()
@click.option('--preserved_indices', default='', help='indices not to be deleted, splited by comma')
@click.option('--only-steps', default=None, help='relocate or delete')
@click.option('--dry-run', is_flag=True)
@click.pass_context
def lifecycle(ctx, preserved_indices, only_steps, dry_run):
    """ indices allocate from hot to warm to cold to delete """
    preserved_indices = preserved_indices.split(',')
    to_warm_indices = {}
    to_cold_indices = {}
    to_delete_indices = set()
    need_force_rollover_indices = set()
    for metadata in get_metadatas('*'):
        if not metadata['policy']:
            continue
        try:
            indices = es.indices.get_settings(metadata['name'], 'index.creation_date,index.number_of_shards,index.number_of_replicas,index.routing.allocation.require.%s' % hot_warm_attribute)
            for k, v in es.indices.get_alias(metadata['name']).items():
                indices[k]['aliases'] = v['aliases']
            for k, v in indices.items():
                try:
                    create_timestamp = int(long(v['settings']['index']['creation_date']) / 1000)
                    hotwarm = deep_get(v['settings']['index'], 'routing', 'allocation', 'require', hot_warm_attribute)
                    if (not only_steps or 'delete' == only_steps) and not fnmatch_list(k, preserved_indices) and now_timestamp - create_timestamp > 86400 * sum(metadata['policy']):
                        if v['aliases'][metadata['name']].get('is_write_index', False):
                            need_force_rollover_indices.add(metadata['name'])
                        else:
                            to_delete_indices.add(k)
                    elif (not only_steps or 'relocate' == only_steps) and len(metadata['policy']) == 3 and hotwarm != 'cold' and \
                            now_timestamp - create_timestamp > 86400 * sum(metadata['policy'][:2]):
                        total_shards_per_node = get_total_shards_per_node(v['settings']['index']['number_of_shards'],
                                                                          v['settings']['index']['number_of_replicas'],
                                                                          get_number_of_nodes(k, 'cold'))
                        to_cold_indices.setdefault(total_shards_per_node, [])
                        to_cold_indices[total_shards_per_node].append(k)
                        if v['aliases'][metadata['name']].get('is_write_index', False):
                            need_force_rollover_indices.add(metadata['name'])
                    elif (not only_steps or 'relocate' in only_steps) and len(metadata['policy']) in [2, 3] and hotwarm != 'warm' and \
                            now_timestamp - create_timestamp > 86400 * metadata['policy'][0]:
                        total_shards_per_node = get_total_shards_per_node(v['settings']['index']['number_of_shards'],
                                                                          v['settings']['index']['number_of_replicas'],
                                                                          get_number_of_nodes(k, 'warm'))
                        to_warm_indices.setdefault(total_shards_per_node, [])
                        to_warm_indices[total_shards_per_node].append(k)
                        if v['aliases'][metadata['name']].get('is_write_index', False):
                            need_force_rollover_indices.add(metadata['name'])
                except Exception as e:
                    click.echo('[%s][error][%s][%s] %s' % (datetime.now().strftime('%Y-%m-%d %H:%M'), metadata['name'], k, e), err=True)
        except Exception as e:
            click.echo('[%s][error][%s][%s] %s' % (datetime.now().strftime('%Y-%m-%d %H:%M'), metadata['name'], k, e), err=True)
    if need_force_rollover_indices:
        click.echo('[%s] need_force_rollover_indices:' % (datetime.now().strftime('%Y-%m-%d %H:%M')))
        click.echo(tabulate(chunks_list(need_force_rollover_indices, 2)))
        ctx.invoke(rollover, name=','.join(need_force_rollover_indices), force=True) if not dry_run else None
    if to_delete_indices:
        click.echo('[%s] to_delete_indices:' % (datetime.now().strftime('%Y-%m-%d %H:%M')))
        click.echo(tabulate(chunks_list(to_delete_indices, 2)))
        if not dry_run:
            for indices_chunk in chunks(to_delete_indices, 50):
                es.indices.delete(index=','.join(indices_chunk))
    if to_cold_indices:
        for total_shards_per_node, indices in to_cold_indices.items():
            click.echo('[%s] to_cold_indices[total_shards_per_node: %d]:' % (datetime.now().strftime('%Y-%m-%d %H:%M'), total_shards_per_node))
            click.echo(tabulate(chunks_list(indices, 2)))
            if not dry_run:
                for indices_chunk in chunks(indices, 50):
                    try:
                        index_list = ','.join(indices_chunk)
                        es.indices.put_settings(index=index_list, body={
                            'index.routing.allocation.require.%s' % hot_warm_attribute: 'cold',
                            'index.routing.allocation.total_shards_per_node': total_shards_per_node
                        })
                        es.indices.delete_alias(index=index_list, name=make_hot_name_pattern())
                    except Exception as e:
                        click.echo('[%s] %s' % (datetime.now().strftime('%Y-%m-%d %H:%M'), e), err=True)
    if to_warm_indices:
        for total_shards_per_node, indices in to_warm_indices.items():
            click.echo('[%s] to_warm_indices[total_shards_per_node: %d]:' % (datetime.now().strftime('%Y-%m-%d %H:%M'), total_shards_per_node))
            click.echo(tabulate(chunks_list(indices, 2)))
            if not dry_run:
                for indices_chunk in chunks(indices, 50):
                    try:
                        index_list = ','.join(indices_chunk)
                        es.indices.put_settings(index=index_list, body={
                            'index.routing.allocation.require.%s' % hot_warm_attribute: 'warm',
                            'index.routing.allocation.total_shards_per_node': total_shards_per_node
                        })
                        es.indices.delete_alias(index=index_list, name=make_hot_name_pattern())
                    except Exception as e:
                        click.echo('[%s] %s' % (datetime.now().strftime('%Y-%m-%d %H:%M'), e), err=True)


@cli.command()
@click.option('--columns', default=None)
def inspect(columns):
    """ inspect names managed by erotate """
    rows = []
    headers = ['name', 'pattern', 'timezone', 'time_field', 'shards_nodes_ratio', 'number_of_replicas', 'rollover_size', 'policy',
               'daily_pri_size', 'daily_size', 'total_pri_size', 'total_size', 'real_policy', 'shard_usage']
    if columns:
        headers = uniq(['name'] + columns.split(','))
    total_pri_size = 0
    total_daily_pri_size = 0
    total_size = 0
    total_daily_size = 0
    for metadata in get_metadatas('*'):
        rollover_size = parse_size(metadata['rollover_size'], True)
        indices_settings = es.indices.get_settings(metadata['name'], 'index.number_of_shards,index.routing.allocation.require.%s' % hot_warm_attribute)
        pri_size = 0
        size = 0
        policy_seconds = {'hot': 0, 'warm': 0, 'cold': 0}
        total_seconds = 0
        shards = 0
        indices = es.cat.indices(metadata['name'], format='json')
        indices_timestamp = get_indices_start_end_timestamp([i['index'] for i in indices], metadata['time_field'])
        for index in indices:
            hotwarm = deep_get(indices_settings[index['index']]['settings']['index'], 'routing', 'allocation', 'require', hot_warm_attribute)
            if hotwarm:
                policy_seconds[hotwarm] += indices_timestamp[index['index']]['duration']
            total_seconds += indices_timestamp[index['index']]['duration']
            _pri_size = parse_size(index['pri.store.size'], True)
            _store_size = parse_size(index['store.size'], True)
            total_pri_size += _pri_size
            total_size += _store_size
            pri_size += _pri_size
            size += _store_size
            shards += int(indices_settings[index['index']]['settings']['index']['number_of_shards'])
        day_count = total_seconds / 86400
        total_daily_pri_size += pri_size / day_count if day_count > 0 else 0
        total_daily_size += size / day_count if day_count > 0 else 0
        metadata['daily_pri_size'] = format_size(pri_size / day_count) if day_count > 0 else 0
        metadata['daily_size'] = format_size(size / day_count) if day_count > 0 else 0
        metadata['total_pri_size'] = format_size(pri_size)
        metadata['total_size'] = format_size(size)
        metadata['real_policy'] = '[%s]' % ', '.join(map(lambda x: '%g' % round(x / 86400, 1), policy_seconds.values()) if sum(policy_seconds.values()) > 0 else ['%g' % round(day_count, 1)])
        metadata['shard_usage'] = "%s%%" % round(float(pri_size) / shards / rollover_size * 100, 2)
        rows.append(OrderedDict([(key, metadata[key]) for key in headers]))
    click.echo(tabulate(rows, headers="keys"))
    click.echo("daily pri-size: %s" % format_size(total_daily_pri_size, True))
    click.echo("daily size: %s" % format_size(total_daily_size, True))
    click.echo("total pri size: %s" % format_size(total_pri_size, True))
    click.echo("total size: %s" % format_size(total_size, True))


if __name__ == '__main__':
    urllib3.disable_warnings()
    cli()
