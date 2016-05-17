package edu.washington.escience.myria.mrbenchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.LocalShuffleConsumer;
import edu.washington.escience.myria.operator.network.LocalShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class AggregateQueryVariantMonetDBMyriaSubStr implements QueryPlanGenerator {

  private static final long serialVersionUID = 1L;
  final static ImmutableList<Type> outputTypes =
      ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> outputColumnNames =
      ImmutableList.of("sourceIPAddr", "sum_adRevenue");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final DbQueryScan localScan =
        new DbQueryScan(
            "select sourceIPAddr, SUM(adRevenue) from UserVisits group by sourceIPAddr",
            outputSchema);

    final int NUM_LOCAL_TASKS = 5;

    PartitionFunction pf0 = new SingleFieldHashPartitionFunction(allWorkers.length, 0);

    PartitionFunction pfLocal0 = new SingleFieldHashPartitionFunction(NUM_LOCAL_TASKS, 0);

    ExchangePairID[] localShuffleIDs = new ExchangePairID[NUM_LOCAL_TASKS];
    for (int i = 0; i < localShuffleIDs.length; i++) {
      localShuffleIDs[i] = ExchangePairID.newID();
    }
    LocalShuffleProducer localsp = new LocalShuffleProducer(localScan, localShuffleIDs, pfLocal0);

    LocalShuffleConsumer[] lsc = new LocalShuffleConsumer[localShuffleIDs.length];
    final GenericShuffleProducer[] shuffleLocalGroupBys = new GenericShuffleProducer[lsc.length];
    final ExchangePairID shuffleLocalGroupByID = ExchangePairID.newID();

    for (int i = 0; i < lsc.length; i++) {
      lsc[i] = new LocalShuffleConsumer(localsp.getSchema(), localShuffleIDs[i]);

      SubStr ss = new SubStr(0, 1, 7);
      ss.setChildren(new Operator[] {lsc[i]});

      final SingleGroupByAggregate localAgg =
          new SingleGroupByAggregate(
              ss,
              1,
              new AggregatorFactory[] {new SingleColumnAggregatorFactory(0, AggregationOp.SUM)});

      shuffleLocalGroupBys[i] =
          new GenericShuffleProducer(localAgg, shuffleLocalGroupByID, allWorkers, pf0);
    }

    final GenericShuffleConsumer sc =
        new GenericShuffleConsumer(
            shuffleLocalGroupBys[0].getSchema(), shuffleLocalGroupByID, allWorkers);

    final SingleGroupByAggregate agg =
        new SingleGroupByAggregate(
            sc,
            0,
            new AggregatorFactory[] {new SingleColumnAggregatorFactory(1, AggregationOp.SUM)});

    final CollectProducer sendToMaster = new CollectProducer(agg, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    RootOperator[] roots = new RootOperator[shuffleLocalGroupBys.length + 2];
    System.arraycopy(shuffleLocalGroupBys, 0, roots, 0, shuffleLocalGroupBys.length);
    roots[shuffleLocalGroupBys.length] = localsp;
    roots[shuffleLocalGroupBys.length + 1] = sendToMaster;

    for (int worker : allWorkers) {
      result.put(worker, roots);
    }

    return result;
  }

  @Override
  public SinkRoot getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    SinkRoot serverPlan = new SinkRoot(serverCollect);
    return serverPlan;
  }
}
