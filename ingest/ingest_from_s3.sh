#!/bin/sh
command -v aws >/dev/null 2>&1 || { echo >&2 "aws-cli not found - aborting."; exit 1; }

if [ "$#" -lt "2" ]; then
  echo "Usage: $0 s3Bucket ResultRelation"
  echo "  ResultRelation: the name of the new relation to store in Myria"
  echo "  ex: $0 oceankmers/overlapped kmercnt_11_ol"
  echo "  This creates a MyriaL query that unions together all the matching relations and stores them."
  exit 1
fi

ResultRelation="${2-}"
s3bucket="$1"


str=""
for f in $(aws s3 ls $s3bucket/ | cut -d ' ' -f 6); do
      sid=`expr "$f" : '.*\(S[0-9]\{4\}\)'`
      s3path="https://s3-us-west-2.amazonaws.com/$s3bucket/$f"
	    if [ -z "$str" ]; then
		    str="$sid = load(\"$s3path\", csv(schema(kmer:string, cnt:float),skip=1)); R = [from $sid emit \"$sid\" as sampleid, kmer, cnt];
"
		    else
		str="$str $sid = load(\"$s3path\", csv(schema(kmer:string, cnt:float),skip=1)); R = R + [from $sid emit \"$sid\" as sampleid, kmer, cnt];
"
		  fi
done

str="$str
store(R, ${ResultRelation}_Pkmer, [kmer]);"

echo "$str"

