#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

_usage() {
  echo "Usage: $0 myria_host:myria_port myria_host:myria_port Relation"
  echo "  The first Myria is the Myria with the relation we want to download from."
  echo "  The second Myria is the Myria we want to copy the relation to."
  echo "  This script copies a relation from one Myria instance to another."
  exit 1
}
# ./sync_tables.sh ec2-52-42-66-221.us-west-2.compute.amazonaws.com:8753 localhost:8753 BC_condensed
# ./sync_tables.sh http://ec2-52-32-167-100.us-west-2.compute.amazonaws.com:8753  node-109:8753 kmercnt_11_forward_Pkmer
# ./sync_tables.sh http://ec2-52-32-167-100.us-west-2.compute.amazonaws.com:8753  node-109:8753 kmercnt_11_sum_Psample

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



SrcMyria="${1}"
DstMyria="${2}"
Relation="${3}"

SrcInfo="${SrcMyria}/dataset/user-public/program-adhoc/relation-${Relation}"
all=$(curl -s -XGET "${SrcInfo}")
schema=$(echo "$all" | jsawk 'return this.schema')
relationKey=$(echo "$all" | jsawk 'return this.relationKey')
echo "schema: $schema"
echo "relationKey: $relationKey"
echo "$schema" > "$TDIR/schema.json"
echo "$relationKey" > "$TDIR/relationKey.json"
# columnTypes=($(echo "$schema" | jsawk 'return this.columnTypes.join(" ")'))
# columnNames=($(echo "$schema" | jsawk 'return this.columnNames.join(" ")'))
# echo "columnTypes: ${columnTypes[*]}"
# echo "columnNames: ${columnNames[*]}"
# for i in "${!columnTypes[@]}"; do 
#   printf "%2d\t%s\t%s\n" "$i" "${columnTypes[$i]}" "${columnNames[$i]}"
# done

curl -s -XGET "${SrcMyria}/dataset/user-public/program-adhoc/relation-${Relation}/data?format=csv" \
  | tail -n +2 \
  | curl -i -XPOST "${DstMyria}"/dataset -H "Content-type: multipart/form-data" \
            -F "relationKey=@$TDIR/relationKey.json; type=application/json" \
            -F "schema=@$TDIR/schema.json; type=application/json" \
            -F "delimiter=," \
            -F "overwrite=$force" \
            -F "binary=false" \
            -F "data=@-; type=application/octet-stream"

