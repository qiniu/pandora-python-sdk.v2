import os
import json
import sys
import unittest
import pdr_python_sdk
from pdr_python_sdk.storage import client

class TestStorageExample(unittest.TestCase):

    def setUp(self):
        """
        Demo connection params
        """
        params = {
            "scheme": os.getenv("PANDORA_SCHEME", "http"),
            "host": os.getenv("PANDORA_HOST"),
            "port": os.getenv("PANDORA_PORT", None),
            "token": os.getenv("PANDORA_TOKEN")
        }
        if not params["host"]:
            raise RuntimeError("PANDORA_HOST must be set")
        if not params["token"]:
            raise RuntimeError("PANDORA_TOKEN must be set")
        self.conn = pdr_python_sdk.connect(**params)
        self.client = client.connect(**params)
        parentdir = os.path.dirname(os.path.abspath(__file__))
        app_path = os.sep.join([parentdir, "data", "pure-app.tar.gz"])
        self.conn.app_install(app_path, overwrite=True)
        
    def tearDown(self):
        self.conn.app_uninstall("test_pure_app")

    def test_create_table(self):
        """
        test table
        """
        storageTable = self.client.storage("test_pure_app")
        self.assertEqual(storageTable.path, '/storage/collections/test_pure_app')

        storageTable = self.client.storage("test_pure_app", version="v2")
        self.assertEqual(storageTable.path, '/storage/v2/collections/test_pure_app')

        response = storageTable.create(data={"tableName":"sdk_new7", "fields":[{"name":"name","type":"varchar","length":128}], "indices":[["name"]]})
        self.assertEqual(response, {})
        
        response = storageTable.get_tables()
        self.assertGreaterEqual(response['total'], 1)

        for collection in response['collections']:
            if collection['app'] == 'test_pure_app' and collection['app_table'] == 'sdk_new7':
                record = collection
        self.assertEqual(record, {'app': 'test_pure_app', 'scope': 'GLOBAL', 'app_table': 'sdk_new7', 'fields': [{'name': 'name', 'type': 'varchar', 'length': 128}], 'indices': [['name']]})
        
        response = storageTable.update(data={"tableName":"sdk_new7", "field":{"name":"age","type":"int"}, "operation":"add"})
        self.assertEqual(response, {})
        
        response = storageTable.update(data={"tableName":"sdk_new7", "field":{"name":"age","type":"int"}, "operation":"delete"})
        self.assertEqual(response, {})

        """
        test data
        """
        storageTableData = storageTable.data("sdk_new7")
        self.assertEqual(storageTableData.path, '/storage/v2/collections/test_pure_app/data/sdk_new7')

        response = storageTableData.insert({"name":"sdk_test1"})
        self.assertEqual(response, {})

        response = storageTableData.batch_save([{"name":"sdk_test2"}])
        self.assertEqual(response, {})

        response = storageTableData.query(query="name='sdk_test1'")
        self.assertEqual(response['data'], [{'id': 1, 'name': 'sdk_test1'}])

        response = storageTableData.query()
        self.assertEqual(response['total'], 2)
        self.assertEqual(response['data'], [{'id': 1, 'name': 'sdk_test1'}, {'id': 2, 'name': 'sdk_test2'}])

        response = storageTableData.query_by_id(1)
        self.assertEqual(response['id'], 1)

        response = storageTableData.updateByQuery({"name":"sdk_test"}, query="id=1")
        self.assertEqual(response, {})

        response = storageTableData.update(1, {"name":"sdk_test3"})
        self.assertEqual(response, {})

        response = storageTableData.delete(query='name="sdk_test2"')
        self.assertEqual(response, {})

        response = storageTableData.delete_by_id(1)
        self.assertEqual(response, {})

        response = storageTable.delete_table("sdk_new7")
        self.assertEqual(response, {})


if __name__ == "__main__":
    unittest.main()
