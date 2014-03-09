import pymongo
from mock import Mock, MagicMock, patch, mock_open, call
#pylint: disable=no-name-in-module
from argparse import Namespace
from nose.tools import assert_true, assert_false, assert_equal, assert_regexp_matches, assert_raises
from migrate_database import (Migration, get_migrations_to_execute, MIGRATION_STATUS_OK,
                              ExitMessage, process_migrations, mongo_db, process,
                              backup_database, check_consecutive)


@patch('glob.glob', Mock(return_value=['./0000_test.js', '0001_test.js']))
@patch('os.path.isfile', Mock(return_value=True))
def test_get_migrations_to_execute():
    migrations_dict = {
        '0000_test.js': Migration('0000_test.js', document=None, script_content=''),
        '0001_test.js': Migration('0001_test.js', document=None, script_content='')
    }

    with patch('migrate_database.Migration.load_migration',
               side_effect=lambda filename: migrations_dict[filename]):
        migrations_to_execute = get_migrations_to_execute()

        assert_equal(migrations_to_execute, [migrations_dict['0000_test.js'], migrations_dict['0001_test.js']])

EMPTY_STRING_HASH = 'da39a3ee5e6b4b0d3255bfef95601890afd80709'
@patch('glob.glob', Mock(return_value=['./0000_test.js', '0001_test.js']))
@patch('os.path.isfile', Mock(return_value=True))
def test_get_migrations_with_scripts_already_applied():
    migrations_dict = {
        '0000_test.js': Migration('0000_test.js',
                                  document={'status': MIGRATION_STATUS_OK, 'sha_hash': EMPTY_STRING_HASH},
                                  script_content=''),
        '0001_test.js': Migration('0001_test.js',
                                  document={'status': MIGRATION_STATUS_OK, 'sha_hash': EMPTY_STRING_HASH},
                                  script_content='')
    }

    with patch('migrate_database.Migration.load_migration',
               side_effect=lambda filename: migrations_dict[filename]):

        migrations = get_migrations_to_execute()

        assert_equal(migrations, [])


@patch('glob.glob', Mock(return_value=[]))
def test_get_migrations_to_execute_with_no_scripts_found():
    with assert_raises(ExitMessage) as exit_message:
        get_migrations_to_execute()

    assert_equal(exit_message.exception.code, 1)
    assert_regexp_matches(exit_message.exception.message, '^No migration scripts in current working directory.*')


def test_remove_script_from_exception_message():
    message = 'command SON(throw ... \' "\\n\\n") failed: invoke failed: JS Error: uncaught exception: does not work'
    assert_equal(Migration.remove_script_from_exception_message(message),
                 'invoke failed: JS Error: uncaught exception: does not work')


def create_mongo_mock(find_returns=None):
    db_mock = MagicMock(name='mongo_client', spec=pymongo.database.Database)

    db_mock.eval = MagicMock()
    db_mock.eval.return_value = None

    db_mock.migrations = MagicMock()
    db_mock.migrations.find.return_value = find_returns
    db_mock.migrations.find_one.return_value = find_returns

    return db_mock


@patch('migrate_database.mongo_db', return_value=create_mongo_mock())
def test_process_migrations(mongo_mock):
    migrations_dict = {
        '0000_test.js': Migration('0000_test.js', document=None, script_content='content1'),
        '0001_test.js': Migration('0001_test.js', document=None, script_content='content2')
    }
    process_migrations([migrations_dict['0000_test.js'], migrations_dict['0001_test.js']])

    assert_equal(mongo_mock().eval.call_args_list, [call('content1'), call('content2')])

    extracted_mongo_insert_status = [c[0][0]['status'] for c in mongo_mock().migrations.insert.call_args_list]
    assert_equal(extracted_mongo_insert_status, [MIGRATION_STATUS_OK, MIGRATION_STATUS_OK])


MIGRATION_TEST_DOCUMENT = {'status': 'OK', 'sha_hash': '0x22', 'filename': '0000_test.js'}
@patch('migrate_database.mongo_db',
       return_value=create_mongo_mock(find_returns=MIGRATION_TEST_DOCUMENT))
@patch('__builtin__.open', mock_open(read_data='migration file content'), create=True)
def test_migration_class(mock_mongo):
    migration = Migration.load_migration('0000_test.js')

    assert_equal(migration.filename, '0000_test.js')
    assert_equal(migration.document, MIGRATION_TEST_DOCUMENT)

    assert_true(migration.inconsistent_hashcode())
    assert_true(migration.was_already_applied())

    migration.apply_to_mongo()
    mock_mongo().eval.assert_called_once_with('migration file content')


@patch('os.makedirs', Mock())
@patch('migrate_database.get_dump_name', Mock(return_value='test.dump'))
@patch('migrate_database.mongo_db', Mock(return_value=create_mongo_mock()))
def test_backup_database():
    with patch('os.system') as mock_system:
        mock_system.return_value = 0
        backup_database()
        mock_system.has_calls('test.dump')

    with patch('os.system') as mock_system:
        mock_system.return_value = 1
        try:
            backup_database()
        except ExitMessage as exit_message:
            assert_equal(exit_message.code, 1)

def test_check_consecutive():
    assert_true(check_consecutive(['0000_test.js', '0001_test.js']))
    assert_true(check_consecutive(['0001_test.js', '0000_test.js', '0002_test.js']))
    assert_false(check_consecutive(['0001_test.js', '0003_test.js']))
