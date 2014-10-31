migrate_mongo
=============

Even if mongo is a schemaless db. We found it useful to migrate all of our collections for bigger db chanes so we do not have to deal with to many different cases in our code. This script implements db maintain like migrations for mongo.

Install
=============
pip install requirements.txt

Running
=============
* Change the config dict in the script to contain your db credentials
* Put your migration scripts into the same dir as the python files
* Execute the script 
    
    python migrate_database.py

Tests
=============
* nosetests test_migrate_database.py

