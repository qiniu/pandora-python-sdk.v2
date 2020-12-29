from pdr_python_sdk.storage import *

if __name__ == "__main__":
    c = connect()
    storageTable = c.storage("cluster_monitoring")
    
    storageTable.create(data='{"name":"sdk_new7", "schema":"des TEXT"}')
    response = storageTable.get_tables()
    print("get_tables response=", str(response))
    
    response = storageTable.delete_table("sdk_new7")
    #response = storageTable.get_tables()
    print("delete_table response=", str(response)) 
    
    storageTable.delete_table("sdk_new8")
    storageTable.create(data='{"name":"sdk_new8", "schema":"title TEXT, des TEXT"}')
    response = storageTable.get_tables()
    
    storageTableData = storageTable.data("sdk_new8")
    response = storageTableData.insert({"fields": "title,des", "values": "\"lihong\",\"sdk_test\""})
    #response = storageTableData.query()
    print("insert sdk_new8 response=", str(response))  
    
    response = storageTableData.query(query="title=\"lihong\"")
    print("query sdk_new8 response=", str(response))
    
    response = storageTableData.updateByQuery('{"query": "title=\\"lihong\\"", "value": "des=\\"sdk_test_new\\""}')
    #response = storageTableData.query(query="title=\"lihong\"")
    print("updateByQuery sdk_new8 response=", str(response))
    
    response = storageTableData.delete(query="title=\"lihong\"")
    #response = storageTableData.query(query="title=\"lihong\"")
    print("delete query sdk_new8 response=", str(response))    
    
    storageTableData = storageTable.data("lihongtest")
    
    response = storageTableData.delete()
    #response = storageTableData.query()
    print("delete response=", str(response))      
    
    response = storageTableData.insert({"fields": "des", "values": "\"sdk_test\""})
    #response = storageTableData.query()
    print("insert response=", str(response))
    
    response = storageTableData.update(1, '{"value": "des=\\"sdk_new\\""}')
    #response = storageTableData.query_by_id(1)
    print("update response=", str(response)) 
    
    #storageTableData.delete_by_id(1)
    #response = storageTableData.query_by_id(1)
    #print("delete_by_id response=", str(response)) 
    
    response = storageTableData.delete(query="id=1")
    print("delete by query response=", str(response))    
    
    response = storageTableData.delete()
    #response = storageTableData.query()
    print("delete response=", str(response))     
    
    response = storageTableData.batch_save(records={"fields": "des", "values": ["\"sdk_test1\"","\"sdk_test2\"","\"sdk_test3\""]})
    #response = storageTableData.query(pageNo=1, pageSize=2)
    print("response=", str(response))   
    response = storageTableData.query(query="des=\"sdk_test1\"")
    print("query response=", str(response))    
    response = storageTableData.query()
    print("batch_save response=", str(response))   