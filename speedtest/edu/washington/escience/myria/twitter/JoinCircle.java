package edu.washington.escience.myria.twitter;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.client.JsonQueryBaseBuilder;
import edu.washington.escience.myria.client.JsonQueryBuilder;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;

public class JoinCircle {

  public static void main(final String[] args) {

    final SingleFieldHashPartitionFunction pfOn0 = new SingleFieldHashPartitionFunction(null, 0);
    final SingleFieldHashPartitionFunction pfOn1 = new SingleFieldHashPartitionFunction(null, 1);
    final RelationKey a = RelationKey.of("jwang", "multiIDB", "a");
    final RelationKey b = RelationKey.of("jwang", "multiIDB", "b");
    final RelationKey c = RelationKey.of("jwang", "multiIDB", "c0");

    JsonQueryBaseBuilder iterateA = new JsonQueryBaseBuilder()//
        .scan(a).setName("scana")//
        .shuffle(pfOn0).setName("sc2a")//
        .beginIterate();

    JsonQueryBaseBuilder iterateB = new JsonQueryBaseBuilder()//
        .scan(b).setName("scanb")//
        .shuffle(pfOn0).setName("shuffle_b")//
        .beginIterate();

    JsonQueryBaseBuilder iterateC = new JsonQueryBaseBuilder()//
        .scan(c).setName("scanc")//
        .shuffle(pfOn0).setName("shuffle_c")//
        .beginIterate();

    iterateA.hashEquiJoin(iterateC.shuffle(pfOn1), new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 })
        .shuffle(pfOn0).endIterate(iterateA);

    iterateA.shuffle(pfOn1).hashEquiJoin(iterateB, new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 })
        .shuffle(pfOn0).endIterate(iterateB);

    iterateC.hashEquiJoin(iterateB.shuffle(pfOn1), new int[] { 1 }, new int[] { 0 }, new int[] { 0 }, new int[] { 1 })
        .shuffle(pfOn0).endIterate(iterateC);

    JsonQueryBuilder finalResult = iterateC.masterCollect();

    System.out.println(finalResult.buildJson());
  }
}
