#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

_usage() {
  echo "Usage: $0 myria_host:myria_port myria_web_host:myria_web_port QueryPrefix QuerySuffix ResultRelation [-2]"
  echo "  QueryPrefix: the search term prefix for relations in Myria to combine"
  echo "  QuerySuffix: the search term suffix for relations in Myria to combine"
  echo "  ResultRelation: the name of the new relation to store in Myria"
  echo "  -2: If not present, ingest the first batch. If present, ingest all batches after the first."
  echo "      Run the script first without the flag, then with -2."
  echo "  ex: $0 node-109:8753 node-109:8080 kmercnt_11_forward_S \"\" kmercnt_11_forward"
  echo "  This script queries a Myria instance for all the relations that match a search term."
  echo "  It creates a MyriaL query that unions together all the matching relations and stores them."
  exit 1
}
# ./combine_tables.sh localhost:8753 kmercnt_11_forward_S _ol kmercnt_11_forward_ol > kmercnt_11_forward_ol
# ./combine_tables.sh localhost:8753 kmercnt_11_rc_S _ol kmercnt_11_rc_ol > kmercnt_11_rc_ol
# ./combine_tables.sh localhost:8753 kmercnt_11_lex_S _ol kmercnt_11_lex_ol > kmercnt_11_lex_ol

# ./combine_tables.sh localhost:8753 localhost:8124 kmercnt_11_rc_S043 _ol kmercnt_11_rc_ol
# ./combine_tables.sh localhost:8753 localhost:8124 kmercnt_11_rc_S _ol kmercnt_11_rc_ol

if [ "$#" -lt "5" ]; then
  _usage
fi

MyriaHostAndPort="${1}"
MyriaWebHostAndPort="${2}"
QueryPrefix="$3"
QuerySuffix="$4"
ResultRelation="$5"
Flag="${6-}" # optional

BatchSize=100

# Check pre-requisite
command -v jsawk >/dev/null 2>&1 || { echo >&2 "I require 'jsawk' but it's not installed. Aborting."; exit 1; }

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

GlobalCounter=0

do_query() {
  if [[ "$counter" -eq "0" ]] && [ "$Flag" != "-2" ] || [[ "$counter" -gt "0" ]] && [ "$Flag" == "-2" ]; then
    # global TDIR
    Query="$1"
    echo $1

    #echo "$Query"
    echo "$Query" > "$TDIR/combine_query.myl"

    CDBG="-o $TDIR/combine_response_$GlobalCounter.log" #"-o /dev/null"
    

    curl -s -D - $CDBG -XPOST "$MyriaWebHostAndPort"/execute -H "Content-type: multipart/form-data" \
          -F "language=myrial" \
          -F "profile=false" \
          -F "multiway_join=false" \
          -F "push_sql=false" \
          -F "query=@$TDIR/combine_query.myl"
  fi

  GlobalCounter=$((GlobalCounter+1))
}


# rnames=$(curl -s -XGET "$MyriaHostAndPort"/dataset/search?q="${QueryPrefix}${QuerySuffix}" \
#     | jsawk 'return this.relationName' -a 'return this.join("\n")')
# echo "$rnames"
# exit 0

str=""
counter=0
while read rn; do {
    if [[ "$rn" != "$QueryPrefix"* ]] || [[ "$rn" != *"$QuerySuffix" ]]; then
      continue
    fi

    sid=`expr "$rn" : '.*\(S[0-9]\{4\}\)'`

    if [ -z "$str" ]; then
      str="$rn = scan($rn); R = [from $rn emit \"$sid\" as sampleid, kmer, cnt];
"
    else
      str="$str$rn = scan($rn); R = R + [from $rn emit \"$sid\" as sampleid, kmer, cnt];
"
    fi

    counter=$((counter+1))
    if [[ "$counter" -eq "$BatchSize" ]]; then
      # process this batch
      str="$str
store(R, ${ResultRelation}_Pkmer, [kmer]);"
#store(R, ${ResultRelation}_Psampleid, [sampleid]);

      do_query "$str"
      # reset
      str="R = scan(${ResultRelation}_Pkmer);
"
      counter=0
    fi

}; done < <(curl -s -XGET "$MyriaHostAndPort"/dataset/search?q="${QueryPrefix}${QuerySuffix}" \
    | jsawk 'return this.relationName' -a 'return this.join("\n")' ) || :


if [[ "$counter" -gt 0 ]]; then
  # process last batch
  str="$str
store(R, ${ResultRelation}_Pkmer, [kmer]);"

  do_query "$str"
fi




# to remove newlines from the output
# | tr '\n' ' ' | less
