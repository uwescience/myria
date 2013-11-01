package edu.washington.escience.myria.twitter;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.client.JsonQueryBaseBuilder;
import edu.washington.escience.myria.client.JsonQueryBuilder;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;

public class ReachabilityQuery {

  public static void main(final String[] args) {

    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(null, 0);
    final RelationKey a = RelationKey.of("jwang", "reachability", "a0");
    final RelationKey g = RelationKey.of("jwang", "reachability", "g");

    JsonQueryBaseBuilder iterateInput = new JsonQueryBaseBuilder()//
        .scan(a).setName("scan2a")//
        .shuffle(pf).setName("shuffle_a")//
        .beginIterate();

    new JsonQueryBaseBuilder()//
        .scan(g).setName("scan2g")//
        .shuffle(pf).setName("shuffle_2g")//
        .hashEquiJoin(iterateInput, new int[] { 0 }, new int[] {}, new int[] { 0 }, new int[] { 1 }).setName("join")//
        .shuffle(pf).setName("shuffle_j")//
        .endIterate(iterateInput);

    JsonQueryBuilder finalResult = iterateInput.count().setName("count").masterCollect();

    System.out.println(finalResult.buildJson());
  }
}
