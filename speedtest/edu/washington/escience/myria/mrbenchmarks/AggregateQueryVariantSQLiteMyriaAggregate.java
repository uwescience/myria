package edu.washington.escience.myria.mrbenchmarks;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.agg.SingleGroupByAggregate;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class AggregateQueryVariantSQLiteMyriaAggregate implements QueryPlanGenerator {

  /**
   *
   */
  private static final long serialVersionUID = 3909701237444119889L;
  final static ImmutableList<Type> outputTypes =
      ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> outputColumnNames =
      ImmutableList.of("sourceIPAddr", "sum_adRevenue");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final static ImmutableList<Type> scanTypes = ImmutableList.of(Type.STRING_TYPE, Type.DOUBLE_TYPE);
  final static ImmutableList<String> scanColumnNames =
      ImmutableList.of("sourceIPAddr", "adRevenue");
  final static Schema scanSchema = new Schema(scanTypes, scanColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final DbQueryScan localScan =
        new DbQueryScan("select substr(sourceIPAddr, 1, 7), adRevenue from UserVisits", scanSchema);

    final SingleGroupByAggregate localAgg =
        new SingleGroupByAggregate(
            localScan, 0, new SingleColumnAggregatorFactory(1, AggregationOp.SUM));

    final ExchangePairID shuffleLocalGroupByID = ExchangePairID.newID();

    PartitionFunction pf = new SingleFieldHashPartitionFunction(allWorkers.length, 0);

    final GenericShuffleProducer shuffleLocalGroupBy =
        new GenericShuffleProducer(localAgg, shuffleLocalGroupByID, allWorkers, pf);
    final GenericShuffleConsumer sc =
        new GenericShuffleConsumer(
            shuffleLocalGroupBy.getSchema(), shuffleLocalGroupByID, allWorkers);

    final SingleGroupByAggregate globalAgg =
        new SingleGroupByAggregate(sc, 0, new SingleColumnAggregatorFactory(1, AggregationOp.SUM));

    final CollectProducer sendToMaster = new CollectProducer(globalAgg, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    for (int worker : allWorkers) {
      result.put(worker, new RootOperator[] {shuffleLocalGroupBy, sendToMaster});
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
