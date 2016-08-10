#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

_usage() {
  echo "Usage: $0 [-f] myria_host:myria_port InputDirectory RelationPrefix RelationSuffix JAVA_COMMAND"
  echo "  InputDirectory: the csv kmer files to ingest. Processed files end in '.csv'"
  echo "  RelationPrefix and RelationSuffix: creates relations of the template \"PrefixSXXXXSuffix\""
  echo "  The first argument can be '-f'. If so, the relation forcefully overwrites existing relations."
  echo "  The JAVA_COMMAND are the arguments to java. It takes files from InputDirectory as input and pipes to a curl POST."
  exit 1
}
#node-109
#/home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt/S0002_11_cnt.csv
#'/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar'
#./ingest_myria.sh -f localhost:8753 /home/dhutchis/gits/istc_oceanography/myria_ingest/S0002_11_cnt_cut.csv kmercnt_11_forward_S0002_cut   -cp /home/dhutchis/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11

# ./ingest_directory.sh localhost:8753 /home/dhutchis/gits/istc_oceanography/extracted-kmers/nohead testpre_ _forward  -cp /home/dhutchis/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11
# ./ingest_directory.sh http://ec2-54-213-84-235.us-west-2.compute.amazonaws.com:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt kmercnt_11_rc_ _ol  -cp /home/gridsan/groups/istcdata/fromDylan/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11 -rc

# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt kmercnt_11_forward_ _ol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt kmercnt_11_rc_ _ol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11 -rc
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_11_cnt kmercnt_11_lex_ _ol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11 -lex
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt kmercnt_11_forward_ _nol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt kmercnt_11_rc_ _nol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11 -rc
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_11_cnt kmercnt_11_lex_ _nol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 11 -lex

# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_13_cnt kmercnt_13_forward_ _ol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 13
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_13_cnt kmercnt_13_rc_ _ol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 13 -rc
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_13_cnt kmercnt_13_lex_ _ol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 13 -lex
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_13_cnt kmercnt_13_forward_ _nol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 13
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_13_cnt kmercnt_13_rc_ _nol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 13 -rc
# ./ingest_directory.sh node-109:8753 /home/gridsan/groups/istcdata/datasets/ocean_metagenome/csv_data/parsed_non_overlapped_13_cnt kmercnt_13_lex_ _nol  -cp /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar edu.mit.ll.graphulo_ocean.OceanPipeKmers  -K 13 -lex

if [ "$#" -lt "5" ]; then
  _usage
fi

force="false"
if [ "$1" == "-f" ]; then
	force="true"
	shift
fi

if [ "$#" -lt "5" ]; then
  _usage
fi

get_script_dir () {
     SOURCE="${BASH_SOURCE[0]}"
     # While $SOURCE is a symlink, resolve it
     while [ -h "$SOURCE" ]; do
          DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
          SOURCE="$( readlink "$SOURCE" )"
          # If $SOURCE was a relative symlink (so no "/" as prefix, need to resolve it relative to the symlink base directory
          [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
     done
     DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
     echo "$DIR"
}
SCRIPT_DIR="$(get_script_dir)"

# Check pre-requisite
command -v jsawk >/dev/null 2>&1 || { echo >&2 "I require 'jsawk' but it's not installed. Aborting."; exit 1; }
command -v "$SCRIPT_DIR/ingest_myria.sh" >/dev/null 2>&1 || { echo >&2 "I require 'ingest_myria' but it's not installed. Aborting."; exit 1; }

MyriaHostAndPort="${1}"
InputDirectory="${2}"
RelationPrefix="${3}"
RelationSuffix="${4}"
shift; shift; shift; shift;

while read fn; do {
	  sid=`expr "$fn" : '.*\(S[0-9]\{4\}\)'`
	  echo "$fn: $sid"

	  if [ "$force" == "true" ]; then
		  "$SCRIPT_DIR/ingest_myria.sh" -f "$MyriaHostAndPort" "$fn" "$RelationPrefix$sid$RelationSuffix" $@
		else
			"$SCRIPT_DIR/ingest_myria.sh" "$MyriaHostAndPort" "$fn" "$RelationPrefix$sid$RelationSuffix" $@
		fi
}; done < <(ls "$InputDirectory"/*S*.csv -1 ) #> /tmp/o

