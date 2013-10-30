## Using toy data

1. Ingest data
    `curl -i -XPOST dbserver02:8755/dataset -H "Content-type: application/json"  -d @./ingest_twitter_small.json`
2. Execute query
    `curl -i -XPOST dbserver02:8755/query -H "Content-type: application/json"  -d @./two_dimension_multiway_join.json`

## Using whole twitter data

1. Ingest data
    `curl -i -XPOST dbserver02:8755/dataset -H "Content-type: application/json"  -d @./ingest_twitter_full.json`
    or
    `curl -i -XPOST dbserver02:8755/dataset/importDataset -H "Content-type: application/json"  -d @./import_twitter_full.json`
2. Execute query
    * two step join 
        `curl -i -XPOST dbserver02:8755/query -H "Content-type: application/json"  -d @./two_step_join.json`
    * hyper cube join
        `curl -i -XPOST dbserver02:8755/query -H "Content-type: application/json"  -d @./hyper_cube_join.json`


## Check the status of workers
    `curl -i dbserver02.cs.washington.edu:8755/workers/alive`
