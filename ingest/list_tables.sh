#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

_usage() {
  echo "Usage: $0 myria_host:myria_port QueryPrefix QuerySuffix"
  echo "  QueryPrefix: the search term prefix for relations in Myria to delete"
  echo "  QuerySuffix: the search term suffix for relations in Myria to delete"
  echo "  ex: $0 node-109:8753 kmercnt_11_forward_S \"\""
  echo "  Lists the relations in a Myria Instance that match a prefix and suffix."
  exit 1
}

if [ "$#" -lt "3" ]; then
  _usage
fi

MyriaHostAndPort="${1}"
QueryPrefix="$2"
QuerySuffix="$3"

# Check pre-requisite
command -v jsawk >/dev/null 2>&1 || { echo >&2 "I require 'jsawk' but it's not installed. Aborting."; exit 1; }

str=""
while read rn; do {

    arr=(${rn})

    if [[ "${arr[2]}" != "$QueryPrefix"* ]] || [[ "${arr[2]}" != *"$QuerySuffix" ]]; then
      continue
    fi

    echo "$rn"

}; done < <(curl -s -XGET "$MyriaHostAndPort"/dataset/search?q="${QueryPrefix}${QuerySuffix}" \
    | jsawk 'return this.userName+" "+this.programName+" "+this.relationName' -a 'return this.join("\n")' ) || :

