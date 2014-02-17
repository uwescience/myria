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

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.client.JsonQueryBaseBuilder;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;

val pfOn0 = new SingleFieldHashPartitionFunction(null, 0);
val pfOn1 = new SingleFieldHashPartitionFunction(null, 1);
val a = RelationKey.of("jwang", "multiIDB", "a0");
val b = RelationKey.of("jwang", "multiIDB", "b0");
val c = RelationKey.of("jwang", "multiIDB", "c0");
val builder = new JsonQueryBaseBuilder().workers(Array(1,2,3))

val iterateA = builder
        .scan(a).setName("scana")
        .shuffle(pfOn0).setName("sc2a")
        .beginIterate();

val iterateB = builder
        .scan(b).setName("scanb")
        .shuffle(pfOn0).setName("shuffle_b")
        .beginIterate();

val iterateC = builder
        .scan(c).setName("scanc")
        .shuffle(pfOn0).setName("shuffle_c")
        .beginIterate();

iterateA.hashEquiJoin(iterateC.shuffle(pfOn1), Array(1), Array(0), Array(0), Array(1))
        .shuffle(pfOn0).endIterate(iterateA);

iterateA.shuffle(pfOn1).hashEquiJoin(iterateB, Array(1), Array(0), Array(0), Array(1))
        .shuffle(pfOn0).endIterate(iterateB);

iterateC.hashEquiJoin(iterateB.shuffle(pfOn1), Array(1), Array(0), Array(0), Array(1))
        .shuffle(pfOn0).endIterate(iterateC);

val finalResult = iterateC.masterCollect();

print(finalResult.buildJson());
