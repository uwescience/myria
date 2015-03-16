from myria import MyriaRelation, MyriaQuery, MyriaConnection, MyriaSchema

def upload_parallel(filename, workers=2):
    print "Uploading" + filename

    types = ['LONG_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE']
    headers = ['col', 'col2', 'col3', 'col4', 'col5']
    schema =  MyriaSchema({'columnTypes': types, 'columnNames': headers})

    connection = MyriaConnection(hostname='localhost', port=8753, ssl=False)

    name_root = filename.split(".")[0]
    relation_name = 'public:adhoc:%s' % (name_root.replace("-","_"))

    relation = MyriaRelation(relation_name, connection=connection, schema=schema)

    work = []
    for i in range(1, workers + 1):
        work.append((i, 'http://s3-us-west-2.amazonaws.com/myria-sdss/astronomy/partioned/%s/p%s/%s' % (name_root, workers,filename + '-part-' + str(i).zfill(5))))

    print work

    #'https://s3-us-west-2.amazonaws.com/myria-sdss/astronomy/partioned/%/p2/%s' % (name_root, filename + '-part-' + str(1).zfill(5))

    #work = [(1, 'https://s3-us-west-2.amazonaws.com/myria-sdss/astronomy/partioned/wise-colors-15-20-subsetsmall256/p2/wise-colors-15-20-subsetsmall256.csv-part-00001'),
    #        (2, 'https://s3-us-west-2.amazonaws.com/myria-sdss/astronomy/partioned/wise-colors-15-20-subsetsmall256/p2/wise-colors-15-20-subsetsmall256.csv-part-00002')
    #        ]
    query = MyriaQuery.parallel_import(relation, work)
    query.wait_for_completion(timeout=3600)
    return name_root.replace("-","_")
    


if __name__ == '__main__':

    types = ['LONG_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE', 'DOUBLE_TYPE']
    headers = ['col', 'col2', 'col3', 'col4', 'col5']
    schema =  MyriaSchema({'columnTypes': types, 'columnNames': headers})
    relation_key = {'userName': 'public',
    'programName': 'test',
    'relationName': 'testMyriaUpload'}

    connection = MyriaConnection(hostname='localhost', port=8753, ssl=False)

    #relation = MyriaRelation('public:adhoc:ocean_seq', connection=connection, schema=schema)
    #work = [(1, 'http://s3-us-west-2.amazonaws.com/myria-sdss/oceanography/ocean_seq.csv'),
    #        (2, 'http://s3-us-west-2.amazonaws.com/myria-sdss/oceanography/ocean_seq.csv')
    #        ]
    relation = MyriaRelation('public:adhoc:AstroSmall', connection=connection, schema=schema)
    work = [(1, 'https://s3-us-west-2.amazonaws.com/myria-sdss/astronomy/partioned/wise-colors-15-20-subsetsmall256/p2/wise-colors-15-20-subsetsmall256.csv-part-00001'),
            (2, 'https://s3-us-west-2.amazonaws.com/myria-sdss/astronomy/partioned/wise-colors-15-20-subsetsmall256/p2/wise-colors-15-20-subsetsmall256.csv-part-00002')
            ]

    query = MyriaQuery.parallel_import(relation, work)
    query.wait_for_completion(timeout=3600)

    print "Done"   
