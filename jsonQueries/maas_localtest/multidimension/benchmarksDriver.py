# coding: utf-8
import myria  # For myria connection
import subprocess  # For calling shell commands
import json  # For importing JSON structures
import os  # For resolving paths
import time  # For getting times

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
def upload_string(relation, path='.'):
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
            print("\t" + status)
            break
        elif status=='KILLED':
            break
        time.sleep(1)
    totalElapsedTime = int((connection.get_query_status(query_id))['elapsedNanos'])
    timeSeconds = totalElapsedTime/1000000000.0
    print('\tRuntime: ' + str(timeSeconds) + ' seconds')
    return timeSeconds


def query_myria(query_string, args=None):
    query = create_json_query("./" + query_string, args)
    query_id = submit_query(query)
    time = monitor_status(query_id)
    return time

# In[46]:

hostname = 'localhost'
port = 8753
connection = myria.MyriaConnection(hostname=hostname, port=port)


# In[47]:

# Parameters of experiment
D = 4
K = 7
n_iter = 1

def run_comparison(n):
    create_test_data(n)
    for i in range(n):
        EMStep()
    compare_results()

def upload_components():
    subprocess.call(upload_string("AstroComponents"), shell=True)
    subprocess.call(upload_string("AstroExpectedComponents"), shell=True)

def create_test_data(n_iterations=1):
    subprocess.call("python ./GMM_Python_Comparison.py %s" % n_iterations, shell=True)

    # Upload the test data
    subprocess.call(upload_string("PointsOnly"), shell=True)
    subprocess.call(upload_string("ComponentsOnly"), shell=True)
    subprocess.call(upload_string("ExpectedComponents"), shell=True)
    subprocess.call(upload_string("ExpectedPoints"), shell=True)

def create_astro_components():
    subprocess.call("python ./GMM_small_astro.py", shell=True)
    upload_components()

def CopyToPoints(relation_name):
    query_myria("CopyRelationToPoints.json", args=(
            relation_name,relation_name,relation_name,relation_name))

def EStepSink():
    # EStep Sink
    query_myria("EStepTemplateSink.json", args=(D,K))

def EStepSink():
    # EStep Sink
    query_myria("EStepTemplateSink.json", args=(D,K))

def PCScanSink():
    # PointsAndComponents Scan Sink
    query_myria("PointsAndComponentsScanSink.json")

def EStepMaterilize():
    # Join tables
    query_myria("Join4D7K.json")

    # EStep Apply
    query_myria("EStepTemplate.json", args=(D,K))

    # EStepAggregate
    query_myria("EStepAggTemplate.json", args=(D,K))

def MStepMaterialize():
    # Join tables
    query_myria("Join4D7K.json")

    # MStepAggregate
    query_myria("MStepTemplate.json", args=(1+D+K,D,K,1+D+K))

def EStep():
    return query_myria("JoinEStep.json")

def MStep():
    return query_myria("JoinMStepNoBroadcast.json")

def MStepNewType():
    return query_myria("JoinMStepNoBroadcastNewType.json")

def EMStepCrossJoin():
    return query_myria("JoinBothSteps.json")

def EMStep():
    return query_myria("joinaggregate/NewENewM.json")

def EMStepXD():
    return query_myria("xd/EMXD.json")
    
def EMStepXDEStep():
    return query_myria("xd/EMXDEStep.json")

def EMDouble():
    return query_myria("joinaggregate/DoubleEM.json")

def EMTriple():
    return query_myria("joinaggregate/TripleEM.json")
    
def pad_points():
    query_myria("PadPoints.json")

def copy_points():
    query_myria("CopyPoints.json")

# Pad the PointsOnly relation with a 4x4 covariance matrix
def add_point_covariance():
    query_myria("xd/addPointCovariance.json")

def astro_components_to_components():
    query_myria("AstroComponentsToComponents.json")

def EMStepNewType():
    return query_myria("newtype/EMNewType.json")

def EMNewTypeNoShuffle():
    return query_myria("newtype/EMNewTypeNoShuffle.json")

def EMNewTypeOneNodeOneFragment():
    return query_myria("newtype/EMNewTypeOneNodeOneFragment.json")

def compare_results():
    #Compare results
    query_myria("CompareComponents4D7K.json")

if __name__ == "__main__":
    start_time = time.time()
    for i in range(n_iter):
        EStep()
        MStep()
    end_time = time.time()
    print "Total time taken: " + str(end_time - start_time) + " seconds."
    compare_results()
