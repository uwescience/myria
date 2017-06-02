# Myria Ingest from `curl`

The following snippet is useful for streaming data into Myria.
Modify it to fit your needs, especially the "relationKey" and "schema".
The `curl` command reads data from stdin. You can pipe data into it from a file or program.

```bash
TDIR="."
MyriaHostAndPort="localhost:8753"
# Write the relation name and schema into temporary files
echo "{ \"userName\" : \"public\", \"programName\" : \"adhoc\", \"relationName\" : \"MyRelation\" }" > "$TDIR/relationKey.json"
echo '{ "columnTypes": ["STRING_TYPE", "LONG_TYPE"], "columnNames": ["kmer", "cnt"] }' > "$TDIR/schema.json"	

curl -i -XPOST "$MyriaHostAndPort"/dataset -H "Content-type: multipart/form-data" \
						-F "relationKey=@$TDIR/relationKey.json; type=application/json" \
						-F "schema=@$TDIR/schema.json; type=application/json" \
						-F "delimiter=," \
						-F "binary=false" \
						-F "overwrite=false" \
						-F "data=@-; type=application/octet-stream"
```

The "binary" option indicates a binary file stream. 
This should come from some equivalent of the Java `DataOutputStream`. See Myria's `BinaryFileScan` class for reference.
Otherwise CSV format is assumed. It should be possible to change the delimiter, 
except that there seems to be trouble communicating the delimiter character to the server.


## Script Starting Points

Included are some scripts that were used in a genomics/oceanography data ingest scenario.
You may find it easier to start with these and modify them to suit your needs.

[ingest_myria.sh](ingest_myria.sh) ingests a single file.

[ingest_directory.sh](ingest_directory.sh) ingests a directory of files, each one as an individual relation.

[combine_tables.sh](combine_tables.sh) generates a MyriaL query that unions together all the dataests that match a search term. It performs the union in batches of 100 relations, for stability. You can change the batch size in the script. 
Run it first normally to submit the first query for execution. Then run it again with flag `-2` to submit the remaining queries, after the first has finished and updated the catalog.

[ingest_from_s3.sh](ingest_from_s3.sh) ingests from S3.

[list_tables.sh](list_tables.sh) lists tables in a Myria instance that match a prefix and suffix.

[delete_tables.sh](delete_tables.sh) deletes (!) tables in a Myria instance that match a prefix and suffix.

[deletetemp.py](deletetemp.py) deletes temporary tables and other crud from the Postgreses in a Myria instance that are left around from failed queries or failed ingests. This is tested on Amazon.

[sync_tables.sh](sync_tables.sh) copies a relation from one Myria instance to another.
