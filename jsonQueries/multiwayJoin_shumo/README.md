1. Ingest data
    `curl -i -XPOST localhost:8753/dataset -H "Content-type: application/json"  -d @./ingest_twitter_small.json`
2. Execute query
    `curl -i -XPOST localhost:8753/query -H "Content-type: application/json"  -d @./two_dimension_multiway_join.json`
