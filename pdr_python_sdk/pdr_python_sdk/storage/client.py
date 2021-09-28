import json
from urllib.parse import urlencode
from pdr_python_sdk.client import PandoraConnection


BASE_V2_PATH = "/storage/v2/collections/{}"
BASE_V1_PATH = "/storage/collections/{}"


__all__ = [
    "connect",
    "Service"
]

"""
:param host: Host name. type ``string``
:param port: Port number. type ``integer``
:param scheme: Scheme for accessing the service. type "http" or "https"
:param token: Access token. type ``string``
:return: A :class:`Service` instance.
"""
def connect(**kwargs):
    return Service(**kwargs)

class Service(PandoraConnection):

    def __init__(self, **kwargs):
        super(Service, self).__init__(**kwargs)
        self._ml_version = None

    def storage(self, app, **kwargs):
        """
        :param app: App name. type ``string``
        :param version: Sdk version. type ``string``
        :return: A :class:`StorageTable`.
        """
        return StorageTable(self, app, **kwargs)
    
class StorageTable(object):
    def __init__(self, service, app, **kwargs):
        self.service = service
        self.app = app
        version = kwargs.get("version", "")
        if version == 'v2':
            self.path = BASE_V2_PATH.format(app)
        else:
            self.path = BASE_V1_PATH.format(app)
        
        
    def data(self, name):
        """
        :param name: Table name. type ``string``
        :return: A :class:`KVStoreCollectionData`.
        """
        return StorageTableData(self.service, self.path, name)

    def create(self, data):
        """
        Create a KV Store table.
        :param data: Table schema. type ``dict`` or ``string``. map with key tableName, fields, indices. The req_body is like:
        >>>{
        >>>    "tableName": "test",
        >>>    "fields": [{"name":"title", "type":"varchar", "length": 128}],
        >>>    "indices": [["title"]]
        >>>}
        :type tableName: ``string``. Table name.
        :type fields: ``list``.  Fields schema. map with key name, type, length.
        :type name: ``string``. Field name.
        :type type: ``string``. Field type, support varchar, int, bigint, timestamp, float, double, text, mediumtext, longtext.
        :type length: ``integer``. Field length, only valid when type is varchar.
        :type indices: ``list``. Table indices.
        :return: Result of POST request
        """
        return self.service.post(self.path + "/config", body=get_object(data))

    def update(self, data):
        """
        Update a KV Store table. Add or delete a field, modify the type or length on a field.
        :param data: Table alter schema. type ``dict`` or ``string``. map with key tableName, field, operation.
        :type tableName: ``string``. Table name.
        :type field: ``dict``. Field Schema, map with key name, type, length, same as create table.
        :type operation: ``string``. Operation type, support add, delete, modify.
        :return: Result of PUT request
        """
        return self.service.put(self.path + "/config", body=get_object(data))
    
    def get_tables(self):
        """
        Get KV Store tables.
        :return: Tables belong to app and all global tables.
        """
        return self.service.get(self.path + "/config")
    
    def delete_table(self, name):
        """
        Delete a KV Store table.
        :param name: Table name. type ``string``.
        :return: Result of DELETE request
        """
        return self.service.delete(self.path + '/{}'.format(name))

class StorageTableData(object):
    """
    Represent the data endpoint for a StorageTable. Using :meth:`StorageTable.data`
    """
    def __init__(self, service, basepath, name):
        self.service = service
        self.path = basepath + '/data/{}'.format(name)

    def query(self, **kwargs):
        """
        Get the results of query.
        :param query: Query string. type ``string``. Will not use other params if query is not empty.
        :param sort: The sort column. type ``string``
        :param order: The order of data, asc or desc, desc by default. type ``string``
        :param pageNo: The page no, start from 1. type ``integer``
        :param pageSize: The size of page, 10 by default. type ``integer``
        :return: Result of documents. rtype: ``dict``
        """
        return self.service.get(self.path, fields=kwargs)

    def query_by_id(self, id):
        """
        Return object with id.
        :param id: Value for ID.
        :return: Document with id. rtype: ``dict``
        """
        return self.service.get(self.path + '/{}'.format(id))

    def insert(self, data):
        """
        Insert record into this table. An id field will be auto generated in the data.
        :param data: Document to insert. type ``dict``. key must in field list. The req_body is like:
        >>>{
        >>>    "title": "test"
        >>>}
        :return: Result of POST request
        """
        return self.service.post(self.path, body=get_object(data))
    
    def delete(self, **kwargs):
        """
        Delete dicuments by query.
        :param query: Query string. type ``string``. will delete all records if query is empty.
        :return: Result of DELETE request
        """
        return self.service.delete(self.path, fields=kwargs)

    def delete_by_id(self, id):
        """
        Delete dicument by id.
        :param id: id record to delete. type ``string``
        :return: Result of DELETE request
        """
        return self.service.delete(self.path + '/{}'.format(id))

    def update(self, id, data):
        """
        Replace record with id and data.
        :param id: Id of record to update. type ``string``
        :param data: The keys to update with new value. type ``dict`` or ``string``
        :return: Result of PUT request
        """
        return self.service.put(self.path + '/{}'.format(id), body=get_object(data))
    
    def updateByQuery(self, data, **kwargs):
        """
        Replace records with id and data.
        :param id: Id of record to update. type ``string``
        :param data: The keys to update with new value. type ``dict`` or ``string``
        :return: Result of PUT request
        """
        if kwargs['insert']:
            kwargs['insert'] = 'true'
        return self.service.put(self.path, body=get_object(data), fields=kwargs)

    def batch_save(self, datas):
        """
        Insert records to table.
        :param datas: Array of records to save as dictionaries. type ``array``
        :return: Result of POST request
        """
        if len(datas) < 1:
            raise Exception('Must have at least one record.')

        return self.service.post(self.path + '/batch_save', body=get_object(datas))

def get_object(element):
    if isinstance(element, str):
        return json.loads(element)
    else:
        return element