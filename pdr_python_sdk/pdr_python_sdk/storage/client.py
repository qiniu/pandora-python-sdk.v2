import json
from ..storage.base import (Base, UrlEncoded)

__all__ = [
    "connect",
    "Service"
]

def _path(base, name):
    if not base.endswith('/'): base = base + '/'
    return base + name

# kwargs: scheme, host, port, app
def connect(**kwargs):
    s = Service(**kwargs)
    return s

class Service(Base):
    """
    :param host: Host name. type ``string``
    :param port: Port number. type ``integer``
    :param scheme: Scheme for accessing the service. type "http" or "https"
    :return: A :class:`Service` instance.
    """
    def __init__(self, **kwargs):
        super(Service, self).__init__(**kwargs)
        self._ml_version = None

    def storage(self, app):
        """
        :param app: App name. type ``string``
        Return KV Store table.
        :return: A :class:`StorageTable`.
        """
        return StorageTable(self, app)


class Endpoint(object):
    """
    An ``Endpoint`` represents a URI.
    This class provides :class:`Entity` (HTTP GET and POST methods).
    """
    def __init__(self, service, path):
        self.service = service
        self.path = path if path.endswith('/') else path + '/'

    def get(self, path_segment="", **kwargs):
        """
        A GET operation on the path segment.
        """
        if path_segment.startswith('/'):
            path = path_segment
        else:
            path = self.service._abspath(self.path + path_segment)
        return self.service.get(path, **kwargs)

    def post(self, path_segment="", body=None):
        """
        A POST operation on the path segment.
        """
        if path_segment == "":
            path = self.path
        elif path_segment.startswith('/'):
            path = path_segment
        else:
            path = self.service._abspath(self.path + path_segment)
        return self.service.post(path, None, body=body)
    
    def put(self, path_segment="", body=None):
        """
        A PUT operation on the path segment.
        """
        if path_segment == "":
            path = self.path
        elif path_segment.startswith('/'):
            path = path_segment
        else:
            path = self.service._abspath(self.path + path_segment)
        return self.service.put(path, None, body=body)    
    
    def delete(self, path_segment="", **kwargs):
        """
        A Delete operation on the path segment.
        """
        if path_segment == "":
            path = self.path
        elif path_segment.startswith('/'):
            path = path_segment
        else:
            path = self.service._abspath(self.path + path_segment)
        return self.service.delete(path, **kwargs)    
    
class StorageTable(Endpoint):
    def __init__(self, service, app):
        Endpoint.__init__(self, service, '/api/v1/storage/collections/' + UrlEncoded(app))
        self.app = app
        
    def data(self, name):
        """
        Return data object for this Collection. rtype: :class:`KVStoreCollectionData`
        """
        return StorageTableData(self, self.app, name)

    def create(self, data):
        """
        Create a KV Store table.

        :param app: App name. type ``string``
        :param name: Table name. type ``string``
        :param schema: Table schema. type ``dict``

        :return: Result of POST request
        """
        return self.post("config", body=data)
    
    def get_tables(self):
        """
        Get a KV Store table.

        :param name: Table name. type ``string``

        :return: Result of POST request
        """
        return json.loads(self.get("config").body)
    
    def delete_table(self, name):
        """
        Get a KV Store table.

        :param name: Table name. type ``string``

        :return: Result of POST request
        """
        return json.loads(self.delete(name).body)     

class StorageTableData(object):
    """
    Represent the data endpoint for a StorageTable. Using :meth:`StorageTable.data`
    """
    def __init__(self, service, app, name):
        self.service = service
        self.path = '/api/v1/storage/collections/' + app +  "/data/" +  name

    def _get(self, url, **kwargs):
        return self.service.get(self.path + url, **kwargs)

    def _post(self, url, body):
        return self.service.post(self.path + url, body=body)
    
    def _put(self, url, body):
        return self.service.put(self.path + url, body=body)    

    def _delete(self, url, **kwargs):
        return self.service.delete(self.path + url, **kwargs)

    def query(self, **kwargs):
        """
        Get the results of query.

        :param kwargs: Parameters (Optional). Such as sort, limit, skip, and fields. type ``dict``
        :return: Array of documents. rtype: ``array``
        """
        return json.loads(self._get('', **kwargs).body)

    def query_by_id(self, id):
        """
        Return object with id.

        :param id: Value for ID.
        :return: Document with id. rtype: ``dict``
        """
        return json.loads(self._get("/" + str(id)).body)

    def insert(self, record):
        """
        Insert item into this table. An id field will be generated in the data.

        :param data: Document to insert. type ``string``
        :return: id of inserted object. rtype: ``dict``
        """
        data = json.dumps(record)
        return json.loads(self._post('', data).body)
    
    def delete(self, **kwargs):
        """
        Delete.

        :param kwargs: Parameters (Optional). Such as sort, limit, skip, and fields. type ``dict``
        :return: Result of DELETE request
        """
        return json.loads(self._delete('', **kwargs).body)    

    def delete_by_id(self, id):
        """
        Delete by id.

        :param id: id record to delete. type ``string``
        :return: Result of DELETE request
        """
        return json.loads(self._delete("/" + str(id)).body)

    def update(self, id, data):
        """
        Replace record with id and data.

        :param id: Id of record to update. type ``string``
        :param data: The new record to insert. type ``string``
        :return: id of replaced record`
        """
        return json.loads(self._put("/" + str(id), body=data).body)
    
    def updateByQuery(self, data):
        """
        Replace record with id and data.

        :param id: Id of record to update. type ``string``
        :param data: The new record to insert. type ``string``
        :return: id of replaced record`
        """
        return json.loads(self._put("", body=data).body)    

    def batch_save(self, records):
        """
        Insert records in records.

        :param records: Array of records to save as dictionaries. type ``array`` of ``dict``
        :return: Results of insert Request.
        """
        if len(records) < 1:
            raise Exception('Must have at least one record.')

        data = json.dumps(records)
        return json.loads(self._post('/batch_save', body=data).body)

