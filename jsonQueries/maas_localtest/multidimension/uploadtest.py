from myria import MyriaRelation, MyriaQuery, MyriaConnection, MyriaSchema

if __name__ == '__main__':

    types = ['LONG_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE']
    headers = ['col', 'col2', 'col3', 'col4', 'col5']
    schema =  MyriaSchema({'columnTypes': types, 'columnNames': headers})
    relation_key = {'userName': 'public',
    'programName': 'test',
    'relationName': 'testMyriaUpload'}

    connection = MyriaConnection(hostname='localhost', port=8753, ssl=False)
    relation = MyriaRelation('public:adhoc:testMyriaUpload', connection=connection, schema=schema)

    work = [(1, 'http://s3-us-west-2.amazonaws.com/myria-sdss/oceanography/smallsample_ocean_seq.csv')]

    query = MyriaQuery.parallel_import(relation, work)
    query.wait_for_completion(timeout=3600)

    print query.to_json()
    
