"""
Script that executes the applicable database migrations on mongo
"""

import argparse
import glob
import hashlib
import logging
import os
import pymongo
import re
import contextlib
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from datetime import datetime


MIGRATION_STATUS_OK = "OK"
MIGRATION_STATUS_FAILED = "FAILED"

LOG = logging.getLogger(__name__)


def init_logging():
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    LOG.setLevel(logging.DEBUG)
    LOG.addHandler(stream_handler)


def mongo_lc_all_workaround():
    if 'LC_ALL' not in os.environ:
        os.environ['LC_ALL'] = 'C'


class ExitMessage(Exception):
    def __init__(self, code, message):
        super(ExitMessage, self).__init__(message)
        self.code = code


@contextlib.contextmanager
def working_dir_set_to_script_dir():
    starting_directory = os.getcwd()
    try:
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        yield
    finally:
        os.chdir(starting_directory)


def already_locked():
    return mongo_db().migrations.find_one({'migration_lock': {'$exists': True}})


@contextlib.contextmanager
def migration_lock():
    if not already_locked():
        mongo_db().migrations.insert({'migration_lock': datetime.utcnow()})
        try:
            yield
        finally:
            mongo_db().migrations.remove({'migration_lock': {'$exists': True}})
    else:
        raise ExitMessage(1, 'Mongo db is already locked by another migrate_database instance!')


@contextlib.contextmanager
def enable_tablescans():
    result = mongo_db().connection.admin.command({'setParameter': 1, 'notablescan': 0})
    if not result[u'ok']:
        ExitMessage(1, 'Disabling "notablescan" option failed"')
    try:
        yield
    finally:
        mongo_db().connection.admin.command({'setParameter': 1, 'notablescan': result[u'was']})


class Migration(object):
    def __init__(self, filename, document=None, script_content=None):
        self.filename = filename
        self.document = document
        self.script_content = script_content

    @staticmethod
    def load_migration(filename):
        migration = Migration(filename)

        with open(filename) as migration_file:
            migration.script_content = migration_file.read()

        migration.document = mongo_db().migrations.find_one({'filename': filename})

        return migration

    def apply_to_mongo(self):
        try:
            error_message = mongo_db().eval(self.script_content)
            if error_message:
                raise RuntimeError(error_message)
        except OperationFailure as operation_failure:
            raise RuntimeError(self.remove_script_from_exception_message(str(operation_failure)))

    def was_already_applied(self):
        if self.document:
            return 'status' in self.document and self.document['status'] == MIGRATION_STATUS_OK

        return False

    def inconsistent_hashcode(self):
        if self.document:
            return hashlib.sha1(self.script_content).hexdigest() != self.document['sha_hash']

        return False

    def save_migration_state(self, status, error_message):
        mongo_db().migrations.insert({'filename': self.filename,
                                      'error_message': error_message,
                                      'status': status,
                                      'sha_hash': hashlib.sha1(self.script_content).hexdigest(),
                                      'script_execution_date': datetime.utcnow()})

    def __str__(self):
        return 'Migration("{}")'.format(self.filename)

    @staticmethod
    def remove_script_from_exception_message(message):
        return re.sub(r'command SON\(.*\) failed: ', '', message)


def mongo_db():
    return db


def init_migrations_collection():
    mongo_db().migrations.create_index([('filename', pymongo.DESCENDING)])
    mongo_db().migrations.create_index([('status', pymongo.DESCENDING)])
    mongo_db().migrations.create_index([('migration_lock', pymongo.DESCENDING)])


def get_dump_name():
    return datetime.utcnow().strftime('dump-%Y-%m-%d-%H-%M-%S')


def backup_database():
    dump_name = get_dump_name()

    try:
        os.makedirs(dump_name)
    except OSError as oserror:
        raise ExitMessage(oserror.errno, oserror.message)

    errno = os.system('mongodump --out {}'.format(dump_name))

    if errno != 0:
        raise ExitMessage(errno, 'failed to backup database')


def check_for_failed_scripts():
    document = mongo_db().migrations.find_one({'status': MIGRATION_STATUS_FAILED})

    if document:
        failed_script = document.get('filename', 'unknown script name, check id ' + str(document['_id']))
        error_message = document.get('error_message', 'unknown error')
        raise ExitMessage(1, 'Last try to migrate the database failed with error "{}". Script with errors "{}". Restore'
                             ' a consistent database state from the backup. '.format(error_message, failed_script))


def extract_filenumber(filename):
    return int(filename[:4])


def check_consecutive(filenames):
    return [extract_filenumber(filename) for filename in sorted(filenames)] == range(0, len(filenames))


def get_migrations_to_execute():
    filenames = [os.path.basename(f) for f in glob.glob('./[0-9][0-9][0-9][0-9]_*.js') if os.path.isfile(f)]

    if not filenames:
        raise ExitMessage(1, 'No migration scripts in current working directory "{}"'.format(os.getcwd()))

    if not check_consecutive(filenames):
        raise ExitMessage(1, 'Non consecutive filename in list "{}"'.format(str(sorted(filenames))))

    migrations_to_execute = []
    for filename in sorted(filenames):
        migration = Migration.load_migration(filename)

        if migration.inconsistent_hashcode():
            raise ExitMessage(1, 'Hash value in db must not differ from '
                                 'hash value for script file "{}"'.format(migration.filename))
        elif migration.was_already_applied():
            LOG.info('migration script "%s" is already applied.', migration.filename)
            continue

        migrations_to_execute.append(migration)

    if not migrations_to_execute:
        LOG.info('No migration scripts needs execution! Database is up to date!')

    return migrations_to_execute


def process_migrations(migrations):
    for migration in migrations:
        LOG.info('Executing migration with name "%s"', migration.filename)
        try:
            migration.apply_to_mongo()
        except RuntimeError as exception:
            migration.save_migration_state(MIGRATION_STATUS_FAILED, exception.message)
            raise ExitMessage(1, 'Execution of database migration "{}" '
                                 'failed with message: "{}"'.format(migration.filename, exception.message))
        else:
            migration.save_migration_state(MIGRATION_STATUS_OK, '')


def process(command_line_options):
    global db
    
    mongo_lc_all_workaround()
    config = {'MONGODB_HOST':'localhost', 'MONGODB_PORT':27017, 'MONGODB_DBNAME': 'test'}

    client = MongoClient(config['MONGODB_HOST'], config['MONGODB_PORT'])
    db = client[config['MONGODB_DBNAME']]

    init_logging()

    LOG.info('Migrating db: "{}"'.format(config['MONGODB_DBNAME']))
    if not command_line_options.disable_backup:
        backup_database()

    init_migrations_collection()

    with migration_lock():
        with enable_tablescans():
            check_for_failed_scripts()

            with working_dir_set_to_script_dir():
                migrations = get_migrations_to_execute()
                process_migrations(migrations)


def main():
    try:
        command_line_options = parse_options()
        process(command_line_options)
    except ExitMessage as exit_message:
        LOG.critical(exit_message.message)
        exit(exit_message.code)

    exit(0)


def parse_options():
    parser = argparse.ArgumentParser(description='Runs and tracks migration on the mongo database')
    parser.add_argument('--disable-backup', action='store_true', help='disables backup of mongo db before migration')
    return parser.parse_args()


if __name__ == '__main__':
    main()



