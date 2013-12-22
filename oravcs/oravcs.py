#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    OraVCS
"""

import os
import re
import subprocess
import logging
import yaml
import shutil
import argparse

import sh
import cx_Oracle

__author__ = 'Pavel Popov'
__email__ = 'pavelpopov@outlook.com'
__license__ = 'GPL'
__version__ = '0.2.2'
__status__ = 'Prototype'


# HOME DIR
ORAVCS_HOME = os.getcwd()

# config dictionary
CONFIG = None

# logging
LOGFORMAT = '%(asctime)s %(levelname)-10s %(funcName)-20s: %(message)s'
LOGLEVEL = logging.DEBUG

logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)


# some various general functions

def key(dict, key, default=None):
    if key in dict:
        return dict[key]
    else:
        return default


def rmdir(path):
    if os.path.isdir(path):
        shutil.rmtree(path)


def mkdir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def debug(fn):
    def inner(*args, **kwargs):
        logger.debug('Running %s', str(fn))

        if args:
            logger.debug('Args: %s', str(args))
        if kwargs:
            logger.debug('Kwargs: %s', str(kwargs))

        try:
            r = fn(*args, **kwargs)
            logger.debug('Finished %s with result %s', str(fn), str(r))
        except Exception, e:
            logger.error('Error at %s occured: %s - %s', str(fn), str(e.args), e.message)

    return inner


def logger_setup(logger, format=LOGFORMAT, datefmt='%Y-%m-%d %H:%M:%S'):
    formatter = logging.Formatter(fmt=format, datefmt=datefmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LOGLEVEL)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(key(CONFIG, 'log', __file__[:-3]+'.log'))
    file_handler.setLevel(LOGLEVEL)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)


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
def install(args):
    """Install Oracle schema objects"""

    basedir = key(CONFIG['install'], 'path', os.path.join(os.path.dirname(__file__), '..', 'install'))
    os.chdir(basedir)

    if args.create_user:
        connection_string = CONFIG['install']['dba']
        datafile = CONFIG['install']['datafile']
        username, password = re.split('\W+', CONFIG['connection'])[0:2]
        sqlplus_exec(connection_string, 'create_user.sql', username, password, datafile)

    # installing schema
    connection_string = CONFIG['connection']
    sqlplus_exec(connection_string, 'build_all.sql')


not_hidden = lambda x, args: x['name'][-1] != '~' and (args.SCHEMA is None or x['name'] in args.SCHEMA)


@debug
def export_schema(schema):
    logger.info('Processing schema %s', schema['name'])

    connection_string = CONFIG['connection']
    schema_dir = os.path.join(ORAVCS_HOME, CONFIG['export'], schema['name'])

    rmdir(schema_dir)
    mkdir(schema_dir)
    
    build_all = open(os.path.join(schema_dir, 'build_all.sql'), 'w')
    build_all_lines = ['WHENEVER OSERROR EXIT 1\n', 'WHENEVER SQLERROR EXIT 2\n']

    ddl_filter = key(schema, 'ddl_filter', CONFIG['ddl_filter'])
    ddl_order = key(schema, 'ddl_order', CONFIG['ddl_order'])

    connection = cx_Oracle.connect(connection_string)
    connection.autocommit = 0
    cursor = connection.cursor()

    if key(schema, 'regenerate', True):
        logger.info('Regenerating object DDLs')
        cursor.execute('DELETE FROM oravcs_metadata WHERE UPPER(schema)=UPPER(:schema)', schema=schema['name'])
        cursor.execute('BEGIN oravcs_process(UPPER(:schema)); END;', schema=schema['name'])
        cursor.execute('COMMIT')

    # logger.debug('Schema: %s', schema)
    query = '''
            SELECT obj_type,
                   obj_name,
                   md
              FROM oravcs_metadata
             WHERE UPPER(schema) = UPPER(:schema)
               AND %s
             ORDER BY %s, id''' % (ddl_filter, ddl_order)
    logger.debug('Query: %s', query)

    cursor.execute(query, schema=schema['name'])

    for row in cursor:
        current_dir = os.path.join(schema_dir, row[0])
        mkdir(current_dir)

        filename = os.path.join(current_dir, '%s.sql' % row[1])

        ddl_file = open(filename, 'w')
        ddl = row[2].read()
        if 'CREATE SEQUENCE' in ddl:
            ddl = re.sub(r'START WITH \d+', 'START WITH 1', ddl)  # replace sequence number
        ddl_file.write(ddl+'\n')  # arbitrary newline at end of file
        ddl_file.close()

        build_all_lines.append('@%s.sql\n' % os.path.join(row[0], row[1]))

        logger.info('Processed file: %s', filename)

    build_all_lines.append('EXIT 0\n')
    build_all.writelines(build_all_lines)

    connection.close()
    build_all.close()


@debug
def export(args):
    map(export_schema, filter(lambda x: not_hidden(x, args), CONFIG['schema']))


@debug
def commit_schema(schema):
    logger.info('Processing schema %s', schema['name'])

    mkdir(os.path.join(ORAVCS_HOME, CONFIG['repos']))

    if 'git' not in schema:
        logger.info('No git setup on schema %s', schema['name'])
        return

    schema_dir = os.path.join(ORAVCS_HOME, CONFIG['repos'], schema['name'])
    export_dir = os.path.join(ORAVCS_HOME, CONFIG['export'], key(schema['git'], 'from_schema', schema['name']), '')
    git = sh.git.bake(_cwd=schema_dir)
    branch = schema['git']['branch']
    repo = schema['git']['repo']

    if repo[0:5] == 'https':
        pass
    else:  # ssh access
        # todo: add support of ssh passphrase
        pass

    if not os.path.exists(schema_dir):  # new repo dir
        try:
            mkdir(schema_dir)
            git.init()
            git.remote.add.origin(repo)
            git.fetch()
            git.checkout(branch)
        except sh.ErrorReturnCode, e:
            rmdir(schema_dir)
            logger.fatal('Error %s', e.args[0])
            return
    else:  # existing repo dir
        try:
            git.reset('--hard')
            git.checkout(branch)
            git.fetch()
            git.pull.origin(branch)
        except sh.ErrorReturnCode, e:
            logger.fatal('Error %s', e.args[0])
            return

    try:
        if not key(schema['git'], 'subdir'):
            dir = os.path.join(schema_dir, '')
        else:
            dir = os.path.join(schema_dir, schema['git']['subdir'])

        sh.rm('-rf', dir)
        mkdir(dir)
        sh.cp('-r', export_dir, dir)
    except:
        raise

    status = git.status('--porcelain')

    if status == '':  # nothing to commit
        logger.info('Nothing to commit - exiting')
        return

    # staging modified files

    def add(item):
        if item[0] == 'D':
            git.rm(item[1])
        else:
            git.add(item[1])

    logger.info('Git status\n%s', git.status())
    lines = [tuple(x.strip().split(' ')) for x in status.split('\n') if x != '']
    map(add, lines)
    logger.info('Git status after staging\n%s', git.status())

    # commit
    git.commit('-m', key(schema['git'], 'comment', 'OraVCS autocommit'))
    logger.info('Commit succeeded')

    if key(schema['git'], 'push', True):
        git.push.origin(branch)
        logger.info('Branch pushed to origin')


@debug
def commit(args):
    map(commit_schema, filter(lambda x: not_hidden(x, args), CONFIG['schema']))


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--config', default='oravcs.yaml', help='YAML config file (default: %(default)s)')

    subparsers = parser.add_subparsers(help='Operation mode')

    def add_schemas_arg(parser):
        parser.add_argument('--schemas', nargs='*', dest='SCHEMA',
                            help='Schemas to be exported. Default - all schemas without ~ at end')

    parser_install = subparsers.add_parser('install', help='Install OraVCS schema into db')
    parser_install.add_argument('--create_user', action='store_true', help='Create user during install process')
    parser_install.set_defaults(func=install)

    parser_export = subparsers.add_parser('export', help='Export schemas from db')
    add_schemas_arg(parser_export)
    parser_export.set_defaults(func=export)

    parser_commit = subparsers.add_parser('commit', help='Commit export result to git')
    add_schemas_arg(parser_commit)
    parser_commit.set_defaults(func=commit)

    args = parser.parse_args()

    global CONFIG
    CONFIG = yaml.load(open(args.config).read())

    logger_setup(logger)

    result = args.func(args)
    return result

if __name__ == '__main__':
    main()
