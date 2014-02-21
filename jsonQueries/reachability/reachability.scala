#!/bin/sh
eclipseProjectRoot=$( cd $(dirname $0) ; pwd -P )
while ! [ -f $eclipseProjectRoot/.classpath ] && ! [ $eclipseProjectRoot = "." ] && ! [ $eclipseProjectRoot = "/" ] && ! [ $eclipseProjectRoot = "" ]; do eclipseProjectRoot=$(dirname $eclipseProjectRoot); done

if ! [ -f $eclipseProjectRoot/.classpath ]; then
  echo "Error:", "Cannot find eclipse .classpath file"
  exit 1;
fi

classpath="$(java -cp $eclipseProjectRoot/build/main  \
  edu.washington.escience.myria.tool.EclipseClasspathReader \
  $eclipseProjectRoot/.classpath)"
exec scala -classpath $classpath "$0" "$@"
!#

import edu.washington.escience.myria.client.JsonQueryBaseBuilder
import edu.washington.escience.myria.RelationKey
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction

val b = new JsonQueryBaseBuilder().workers(Array(1,2))
val pf = new SingleFieldHashPartitionFunction(null,0)
val a = RelationKey.of("jwang","reachability","a0")
val g = RelationKey.of("jwang","reachability","g")

val iterateInput = b.scan(a).setName("scan2a").shuffle(pf).setName("shuffle_a").beginIterate()
b.scan(g).setName("scan2g").shuffle(pf).setName("shuffle_2g").hashEquiJoin(iterateInput,Array(0), Array(1), Array(0), Array()).setName("join").shuffle(pf).setName("shuffle_j").endIterate(iterateInput)
val finalResult = iterateInput.count().setName("count").masterCollect
print(finalResult.buildJson())

