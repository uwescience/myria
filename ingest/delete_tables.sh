#!/bin/bash
set -e #command fail -> script fail
set -u #unset variable reference causes script fail

_usage() {
  echo "Usage: $0 myria_host:myria_port QueryPrefix QuerySuffix"
  echo "  QueryPrefix: the search term prefix for relations in Myria to delete"
  echo "  QuerySuffix: the search term suffix for relations in Myria to delete"
  echo "  ex: $0 node-109:8753 kmercnt_11_forward_S \"\""
  echo "  Be careful with this script!"
  exit 1
}
# ./combine_tables.sh localhost:8753 kmercnt_11_forward_S _ol kmercnt_11_forward_ol > kmercnt_11_forward_ol
# ./combine_tables.sh localhost:8753 kmercnt_11_rc_S _ol kmercnt_11_rc_ol > kmercnt_11_rc_ol
# ./combine_tables.sh localhost:8753 kmercnt_11_lex_S _ol kmercnt_11_lex_ol > kmercnt_11_lex_ol

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

    echo "DELETE: /user-${arr[0]}/program-${arr[1]}/relation-${arr[2]}/"
    curl -XDELETE "$MyriaHostAndPort/dataset/user-${arr[0]}/program-${arr[1]}/relation-${arr[2]}/"

}; done < <(curl -s -XGET "$MyriaHostAndPort"/dataset/search?q="${QueryPrefix}${QuerySuffix}" \
    | jsawk 'return this.userName+" "+this.programName+" "+this.relationName' -a 'return this.join("\n")' ) || :

