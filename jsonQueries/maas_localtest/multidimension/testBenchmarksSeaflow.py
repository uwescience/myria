
# coding: utf-8

# In[43]:

import myria  # For myria connection
import subprocess  # For calling shell commands
import json  # For importing JSON structures
import os  # For resolving paths
import time  # For getting times


# In[44]:

def upload_manually():
    # Example upload of a small dataset. Not to be used for more than 10,000 tuples
    types = ['LONG_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE']
    headers = ['col', 'col2', 'col3', 'col4', 'col5', 'col6']
    schema = {'columnTypes': types, 'columnNames': headers}
    relation_key = {'userName': 'public',
    'programName': 'test',
    'relationName': 'testMyriaUpload'}
    # with open ("/Users/maas/devlocal/myria/jsonQueries/maas_localtest/rawpoints3comp.csv", "r") as myfile:
    #    data=myfile.read()

    #connection.upload_file(relation_key, schema, data, overwrite=True)
def upload_string(relation, path='/Users/maas/devlocal/myria/jsonQueries/maas_localtest/multidimension/'): 
    return "myria_upload --hostname localhost --port 8753 --no-ssl --overwrite --relation %s %s/%s.csv" % (relation, path, relation)


# In[54]:

# Create a json_query in python dictionary format
def create_json_query(path, args=None):
    if args is None:
        args = tuple()
    json_file = open(os.path.expanduser(path))
    textdata = json_file.read() % args
    json_file.close()
    print "Created query '%s'" % (path.split("/")[-1])
    return json.loads(textdata)

# Submit a query
def submit_query(json_query):
    try:
        query_status = connection.submit_query(json_query)
        #query_url = 'http://%s:%d/execute?query_id=%d' % (hostname, port, query_status['queryId'])
        query_id = query_status['queryId']
        #print(json.dumps(connection.get_query_status(query_id)))
    except myria.MyriaError as e:
        print("MyriaError")
        print("Couldn't submit query")
    return query_id

# Check for completion
def monitor_status(query_id):
    print "Query status:"
    status = connection.get_query_status(query_id)['status']
    while status!='SUCCESS':
        status = (connection.get_query_status(query_id))['status']
        #print("\t" + status);
        if status=='SUCCESS':
            print("\t" + status);
            break;
        elif status=='KILLED':
            break;
        time.sleep(1)
    totalElapsedTime = int((connection.get_query_status(query_id))['elapsedNanos'])
    timeSeconds = totalElapsedTime/1000000000.0;
    print('\tRuntime: ' + str(timeSeconds) + ' seconds');


# In[46]:

hostname = 'localhost'
port = 8753
connection = myria.MyriaConnection(hostname=hostname, port=port)


# In[47]:

# Parameters of experiment
D = 4
K = 7
n_iter = 2
#subprocess.call("python ./GMM_Python_Comparison.py %s" % n_iter, shell=True)
#
#
## In[48]:
#
## Upload the test data
#subprocess.call(upload_string("PointsOnly"), shell=True)
#subprocess.call(upload_string("ComponentsOnly"), shell=True)
#subprocess.call(upload_string("ExpectedComponents"), shell=True)
#subprocess.call(upload_string("ExpectedPoints"), shell=True)


# In[60]:

start_time = time.time()
for i in range(n_iter):
    # Join tables
    query = create_json_query(
        "./Join4D7K.json")
    query_id = submit_query(query)
    monitor_status(query_id)
    # EStep Apply
    query = create_json_query(
        "./EStepTemplate.json", args=(D,K))
    query_id = submit_query(query)
    monitor_status(query_id)

    # EStepAggregate
    query = create_json_query(
        "./EStepAggTemplate.json", args=(D,K))
    query_id = submit_query(query)
    monitor_status(query_id)
    # Join tables
    query = create_json_query(
        "./Join4D7K.json")
    query_id = submit_query(query)
    monitor_status(query_id)
    # MStepAggregate
    query = create_json_query(
        "./MStepTemplate.json", args=(1+D+K,D,K,1+D+K))
    query_id = submit_query(query)
    monitor_status(query_id)
end_time = time.time()
print "Total time taken: " + str(end_time - start_time) + " seconds."


# In[50]:

# Compare results
# query = create_json_query(
#     "/Users/maas/devlocal/myria/jsonQueries/maas_localtest/CompareComponents4D7K.json")
# query_id = submit_query(query)
# monitor_status(query_id)


# In[ ]:



