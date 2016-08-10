#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

_usage() {
  echo "Usage: $0 myria_host:myria_port QueryTerm ResultRelation"
  echo "  QueryTerm: the search term for relations in Myria to combine"
  echo "  ResultRelation: the name of the new relation to store in Myria"
  echo "  ex: $0 node-109:8753 kmercnt_11_forward_S kmercnt_11_forward"
  echo "  This script queries a Myria instance for all the relations that match a search term."
  echo "  It creates a MyriaL query that unions together all the matching relations and stores them."
  exit 1
}

if [ "$#" -lt "3" ]; then
  _usage
fi

MyriaHostAndPort="${1-node-109:8753}"
QueryTerm="${2-kmercnt_11_forward_S000}"
ResultRelation="${3-}"

# Check pre-requisite
command -v jsawk >/dev/null 2>&1 || { echo >&2 "I require 'jsawk' but it's not installed. Aborting."; exit 1; }

str=""
while read rn; do {

    sid=`expr "$rn" : '.*\(S[0-9]\{4\}\)'`

    if [ -z "$str" ]; then
      str="$rn = scan($rn); R = [from $rn emit \"$sid\" as sampleid, kmer, cnt];
"
    else
      str="$str$rn = scan($rn); R = R + [from $rn emit \"$sid\" as sampleid, kmer, cnt];
"
    fi
}; done < <(curl -s -XGET "$MyriaHostAndPort"/dataset/search?q="$QueryTerm" \
    | jsawk 'return this.relationName' -a 'return this.join("\n")' ) || :

str="$str
store(R, ${ResultRelation}_Pkmer, [kmer]);
store(R, ${ResultRelation}_Psampleid, [sampleid]);"

echo "$str"

# to remove newlines from the output
# | tr '\n' ' ' | less
