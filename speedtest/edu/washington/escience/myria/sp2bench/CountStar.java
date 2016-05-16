package edu.washington.escience.myria.sp2bench;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.agg.Aggregate;
import edu.washington.escience.myria.operator.agg.AggregatorFactory;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class CountStar implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
  final static ImmutableList<String> outputColumnNames =
      ImmutableList.of("count(*) Dictionary", "count(*) Triples");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final static ImmutableList<Type> countTypes = ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE);
  final static ImmutableList<String> countColumnNames = ImmutableList.of("count", "dummy");

  final static Schema countSchema = new Schema(countTypes, countColumnNames);
  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {
    final ExchangePairID collectCountID = ExchangePairID.newID();

    final DbQueryScan countDictionary =
        new DbQueryScan("select count(*),0 from Dictionary", countSchema);
    final DbQueryScan countTriples = new DbQueryScan("select count(*),0 from Triples", countSchema);

    final SymmetricHashJoin countMergeJoin =
        new SymmetricHashJoin(countDictionary, countTriples, new int[] {1}, new int[] {1});

    final CollectProducer collectCountP =
        new CollectProducer(countMergeJoin, collectCountID, allWorkers[0]);
    final CollectConsumer collectCountC =
        new CollectConsumer(collectCountP.getSchema(), collectCountID, allWorkers);

    final Aggregate agg =
        new Aggregate(
            collectCountC,
            new AggregatorFactory[] {
              new SingleColumnAggregatorFactory(0, AggregationOp.SUM),
              new SingleColumnAggregatorFactory(2, AggregationOp.SUM)
            });

    final CollectProducer sendToMaster = new CollectProducer(agg, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    result.put(allWorkers[0], new RootOperator[] {sendToMaster, collectCountP});

    for (int i = 1; i < allWorkers.length; i++) {
      result.put(allWorkers[i], new RootOperator[] {collectCountP});
    }

    return result;
  }

  @Override
  public RootOperator getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, sendToMasterID, new int[] {allWorkers[0]});
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    return serverPlan;
  }

  public static void main(String[] args) throws Exception {
    System.out.println(new CountStar().getWorkerPlan(new int[] {1, 2, 3, 4, 5}));
  }
}
