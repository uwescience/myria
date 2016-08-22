#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

_usage() {
  echo "Usage: $0 [-f] myria_host:myria_port InputFile ResultRelation JAVA_COMMAND"
  echo "  InputFile: the csv kmer file to ingest"
  echo "  ResultRelation: the name of the new relation to store in Myria"
  echo "  The first argument can be '-f'. If so, the relation is force added."
  echo "  The JAVA_COMMAND are the arguments to java. It takes the InputFile as input and pipes to a curl POST."
  exit 1
}
#node-109
#/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt/S0002_11_cnt.csv
#'/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar'
#./ingest_myria.sh -f localhost:8753 /home/dhutchis/gits/istc_oceanography/myria_ingest/S0002_11_cnt_cut.csv kmercnt_11_forward_S0002_cut   -cp /home/dhutchis/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11
#./ingest_myria.sh -f http://ec2-52-43-110-122.us-west-2.compute.amazonaws.com:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt/S0002_11_cnt.csv test_S0002_cut   -cp '/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar' edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11 -binary

if [ "$#" -lt "3" ]; then
  _usage
fi

force="false"
if [ "$1" == "-f" ]; then
	force="true"
	shift
fi

if [ "$#" -lt "3" ]; then
  _usage
fi

binary=false
if [[ "$@" = *-binary* ]]; then 
	binary=true
fi

# Check pre-requisite
command -v jsawk >/dev/null 2>&1 || { echo >&2 "I require 'jsawk' but it's not installed. Aborting."; exit 1; }

MyriaHostAndPort="${1}"
InputFile="${2}"
ResultRelation="${3}"
shift; shift; shift;

if [ "$force" == "true" ]; then
	t=""
else
	t=$(curl -s -XGET "$MyriaHostAndPort"/dataset/search?q="$ResultRelation" \
		| jsawk 'return this.relationName' -a 'return this.join("\n")' ) 
fi

if [ ! -z "$t" ] && [ "$t" == "$ResultRelation" ]; then
	echo "The relation $ResultRelation already exists."
else
	# do the ingest

	if [ -e "/tmp/myriaIngestDir_$(whoami)" ]; then
		if [ -d "/tmp/myriaIngestDir_$(whoami)" ]; then
			TDIR="/tmp/myriaIngestDir_$(whoami)" 
		else
			echo "Warning: the path \"/tmp/myriaIngestDir\" is taken"
			TDIR=`mktemp -d myriaIngestDir_XXXXXXXXXX`
		fi
	else
		mkdir "/tmp/myriaIngestDir_$(whoami)"
		TDIR="/tmp/myriaIngestDir_$(whoami)"
	fi
	echo "{ \"userName\" : \"public\", \"programName\" : \"adhoc\", \"relationName\" : \"$ResultRelation\" }" > "$TDIR/relationKey.json"
	echo '{ "columnTypes": ["STRING_TYPE", "LONG_TYPE"], "columnNames": ["kmer", "cnt"] }' > "$TDIR/schema.json"

	java $@ < "$InputFile" \
		| curl -i -XPOST "$MyriaHostAndPort"/dataset -H "Content-type: multipart/form-data" \
						-F "relationKey=@$TDIR/relationKey.json; type=application/json" \
						-F "schema=@$TDIR/schema.json; type=application/json" \
						-F "delimiter=," \
						-F "overwrite=$force" \
						-F "binary=$binary" \
						-F "data=@-; type=application/octet-stream"

fi




##################################
	# -F "partitionFunction=a" \

# if [ ! -d ingestTmp ]; then
# 	mkdir ingestTmp
# fi
# echo "1,2
# 2,3
# 4,5
# 6,7" > ingestTmp/smallerTable.csv
# echo '{ "userName" : "public", "programName" : "adhoc", "relationName" : "test1" }' > ingestTmp/relationKey.json
# echo '{ "columnTypes": ["LONG_TYPE", "LONG_TYPE"], "columnNames": ["col1", "col2"] }' > ingestTmp/schema.json

# #http://ec2-52-39-96-185.us-west-2.compute.amazonaws.com
# curl -i -XPOST node-109:8753/dataset -H "Content-type: multipart/form-data" \
# 	-F "relationKey=@./ingestTmp/relationKey.json; type=application/json" \
# 	-F "schema=@./ingestTmp/schema.json; type=application/json" \
# 	-F "overwrite=true" \
# 	-F "data=@-; type=application/octet-stream"

# ./ingestTmp/smallerTable.csv
# 	-F "binary=false" \
# 	-F "isLittleEndian=false" \
# 	-F "delimitter=," \


