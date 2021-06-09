from pdr_python_sdk.storage import *

if __name__ == "__main__":
    c = connect(host="127.0.0.1", port="9999", scheme="http", token="")
    storageTable = c.storage("cluster_monitoring", version="v2")

    # support tyep: varchar,int,timestamp,float,double,text,mediumtext,longtext,bigint
    storageTable.create(data='{"tableName":"sdk_new7", "fields":[{"name":"name","type":"varchar","length":128}], "indices":[["name"]]}')
    response = storageTable.get_tables()
    print("get_tables response=", str(response))

    response = storageTable.delete_table("sdk_new7")
    print("delete_table response=", str(response)) 

    storageTable.create(data='{"tableName":"sdk_new8", "fields":[{"name":"name","type":"varchar","length":128}], "indices":[["name"]]}')
    response = storageTable.get_tables()

    storageTable.update(data='{"tableName":"sdk_new8", "field":{"name":"age","type":"int"}, "operation":"add"}')
    response = storageTable.get_tables()
    print("update sdk_new8 response1=", str(response))

    storageTable.update(data='{"tableName":"sdk_new8", "field":{"name":"age","type":"double"}, "operation":"modify"}')
    response = storageTable.get_tables()
    print("update sdk_new8 response2=", str(response))

    storageTable.update(data='{"tableName":"sdk_new8", "field":{"name":"age"}, "operation":"delete"}')
    response = storageTable.get_tables()
    print("update sdk_new8 response3=", str(response))

    storageTableData = storageTable.data("sdk_new8")
    response = storageTableData.insert({"name":"lihong"})
    print("insert sdk_new8 response=", str(response))  

    response = storageTableData.query(query='name="lihong"')
    print("query sdk_new8 response=", str(response))

    response = storageTableData.updateByQuery('{"name":"qiniu"}', query='name="lihong"')
    print("updateByQuery sdk_new8 response=", str(response))

    response = storageTableData.delete(query='name="qiniu"')
    print("delete query sdk_new8 response=", str(response))    

    storageTableData = storageTable.data("sdk_new8")

    response = storageTableData.delete()
    print("delete response=", str(response))      

    response = storageTableData.insert({"name":"sdk_test"})
    print("insert response=", str(response))

    response = storageTableData.update(1, '{"name": "sdk_new"}')
    print("update response=", str(response)) 

    response = storageTableData.delete(query='id=1')
    print("delete by query response=", str(response))    

    response = storageTableData.delete()
    print("delete response=", str(response))     

    response = storageTableData.batch_save([{"name":"sdk_test1"},{"name":"sdk_test2"},{"name":"sdk_test3"}])
    print("response=", str(response))   

    response = storageTableData.query(query='name="sdk_test1"')
    print("query response=", str(response))

    response = storageTableData.query()
    print("batch_save response=", str(response))

    response = storageTable.delete_table("sdk_new8")
    print("delete sdk_new8 response=", str(response))
