#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    OraVCS
"""

import sys
import os
import re
import subprocess
import logging
import yaml

import shutil
import argparse

import cx_Oracle

__author__ = 'Pavel Popov'
__email__ = 'pavelpopov@outlook.com'
__license__ = 'GPL'
__version__ = '0.1.0'
__status__ = 'Prototype'


# logging
LOGFORMAT = '%(asctime)s %(levelname)-10s %(module)-10s %(funcName)-20s: %(message)s'
LOGLEVEL = logging.DEBUG

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)


def logger_setup(logger, format=LOGFORMAT, datefmt='%Y-%m-%d %H:%M:%S'):
    formatter = logging.Formatter(fmt=format, datefmt=datefmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOGLEVEL)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(__file__[:-3]+'.log')
    file_handler.setLevel(LOGLEVEL)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)


def debug(fn):
    def inner(*args, **kwargs):
        logger.debug('Running %s', str(fn))
        logger.debug('Args: %s', str(args))
        logger.debug('Kwargs: %s', str(kwargs))
        r = fn(*args, **kwargs)
        logger.debug('%s finished with result %s', str(fn), str(r))

    return inner


@debug
def sqlplus_exec(connection_string, script, *args):
    # logger.debug('script: %s', script)
    # logger.debug('path: %s', os.path.expandvars('$PATH'))
    # logger.debug('oracle_home: %s', os.path.expandvars('$ORACLE_HOME'))
    # logger.debug('tns_admin: %s', os.path.expandvars('$TNS_ADMIN'))
    cmd = ['sqlplus', connection_string, '@'+script] + list(args)
    logger.debug('command: %s', ' '.join(cmd))
    result = subprocess.call(cmd)
    return result


@debug
def install(config, **kwargs):
    """Install Oracle schema objects"""

    if 'basedir' in kwargs:
        basedir = kwargs['basedir']
    else:
        basedir = os.path.join(os.path.dirname(__file__), '..', 'install')

    os.chdir(basedir)

    if 'create_user' in kwargs:
        connection_string = config['oravcs']['install']['dba']
        datafile = config['oravcs']['install']['datafile']
        username, password = re.split('\W+', config['oravcs']['connection'])[0:2]
        sqlplus_exec(connection_string, 'create_user.sql', username, password, datafile)

    # installing schema
    connection_string = config['oravcs']['connection']
    sqlplus_exec(connection_string, 'build_all.sql')


def load_config(filename='oravcs.yaml'):
    return yaml.load(open(filename).read())


def rmdir(path):
    if os.path.isdir(path):
        shutil.rmtree(path)


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def fill_param(param_name, param_value, arr):
    return param_value if param_name not in arr else arr[param_name]

@debug
def export_schema(config, schema):
    basedir = config['oravcs']['export']
    connection_string = config['oravcs']['connection']


    schema_name = schema['name']
    logger.info('Processing schema %s', schema_name)
    schema_dir = os.path.join(basedir, schema_name)
    rmdir(schema_dir)
    mkdir(schema_dir)
    build_all = open(os.path.join(schema_dir, 'build_all.sql'), 'w')
    build_all_lines = ['WHENEVER OSERROR EXIT 1\n', 'WHENEVER SQLERROR EXIT 2\n']

    if 'ddl_filter' in schema:
        ddl_filter = schema['ddl_filter']
    else:
        ddl_filter = config['oravcs']['ddl_filter']

    if 'ddl_order' in schema:
        ddl_order = schema['ddl_order']
    else:
        ddl_order = config['oravcs']['ddl_order']

    connection = cx_Oracle.connect(connection_string)
    connection.autocommit = 0
    cursor = connection.cursor()

    if 'download_only' not in schema or not schema['download_only']:
        cursor.execute('DELETE FROM oravcs_metadata WHERE UPPER(schema)=UPPER(:schema)', schema=schema['name'])
        cursor.execute('BEGIN oravcs_process(UPPER(:schema)); END;', schema=schema['name'])
        cursor.execute('COMMIT')

    logger.debug('Schema: %s', schema)
    query = '''
            SELECT obj_type,
                   obj_name,
                   md
              FROM oravcs_metadata
             WHERE UPPER(schema) = UPPER(:schema)
               AND %s
             ORDER BY %s, id''' % (ddl_filter, ddl_order)
    # logger.debug('Query: %s', query)

    cursor.prepare(query)
    cursor.execute(None, {'schema': schema['name']})

    for row in cursor:
        current_dir = os.path.join(schema_dir, row[0])
        mkdir(current_dir)

        filename = os.path.join(current_dir, '%s.sql' % row[1])

        ddl_file = open(filename, 'w')
        ddl_file.write(row[2].read())
        ddl_file.close()

        build_all_lines.append('@%s.sql\n' % os.path.join(row[0], row[1]))

        logger.debug('Processed file: %s', filename)

    build_all_lines.append('EXIT 0\n')
    build_all.writelines(build_all_lines)

    connection.close()
    build_all.close()


@debug
def export(config):
    es = lambda x: export_schema(config=config, schema=x)
    not_hidden = lambda x: x['name'][-1] != '~'
    map(es, filter(not_hidden, config['oravcs']['schema']))


@debug
def main():
    config = load_config()
    # install(config, create_user=True)  # tested - works
    # install(config)
    export(config)

if __name__ == '__main__':
    logger_setup(logger)
    main()
