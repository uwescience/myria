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

The first is for ingesting a single file: [ingest_myria.sh](ingest_myria.sh).
The second is for ingseting a directory of files, each one as an individual relation: [ingest_directory.sh](ingest_directory.sh).
The third generates a MyriaL query that unions together all the dataests that match a search term: [combine_tables.sh](combine_tables.sh).
The fourth is for ingesting from S3: [ingest_from_s3.sh](ingest_from_s3.sh).


