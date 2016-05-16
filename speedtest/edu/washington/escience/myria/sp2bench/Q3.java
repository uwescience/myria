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
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;
import edu.washington.escience.myria.operator.agg.SingleColumnAggregatorFactory;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class Q3 implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.LONG_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("count");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final static ImmutableList<Type> subjectTypes = ImmutableList.of(Type.LONG_TYPE);
  final static ImmutableList<String> subjectColumnNames = ImmutableList.of("subject");

  final static Schema subjectSchema = new Schema(subjectTypes, subjectColumnNames);
  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {
    final ExchangePairID allArticlesShuffleID = ExchangePairID.newID();
    final ExchangePairID allSwrcPagesShuffleID = ExchangePairID.newID();
    final ExchangePairID collectCountID = ExchangePairID.newID();

    final DbQueryScan allArticles =
        new DbQueryScan(
            "select t.subject from Triples t, Dictionary dtype, Dictionary darticle where t.predicate=dtype.id and t.object=darticle.id and darticle.val='bench:Article' and dtype.val='rdf:type';",
            subjectSchema);

    final DbQueryScan allWithSwrcPages =
        new DbQueryScan(
            "select t.subject from Triples t,Dictionary dtype where t.predicate=dtype.ID and dtype.val='swrc:pages';",
            subjectSchema);

    final SingleFieldHashPartitionFunction pf =
        new SingleFieldHashPartitionFunction(allWorkers.length, 0);

    final GenericShuffleProducer shuffleArticlesP =
        new GenericShuffleProducer(allArticles, allArticlesShuffleID, allWorkers, pf);
    final GenericShuffleConsumer shuffleArticlesC =
        new GenericShuffleConsumer(shuffleArticlesP.getSchema(), allArticlesShuffleID, allWorkers);

    final GenericShuffleProducer shuffleSwrcPagesP =
        new GenericShuffleProducer(allWithSwrcPages, allSwrcPagesShuffleID, allWorkers, pf);
    final GenericShuffleConsumer shuffleSwrcPagesC =
        new GenericShuffleConsumer(
            shuffleSwrcPagesP.getSchema(), allSwrcPagesShuffleID, allWorkers);

    final SymmetricHashJoin joinArticleSwrcPages =
        new SymmetricHashJoin(shuffleArticlesC, shuffleSwrcPagesC, new int[] {0}, new int[] {0});

    final Aggregate agg =
        new Aggregate(
            joinArticleSwrcPages, new SingleColumnAggregatorFactory(0, AggregationOp.COUNT));

    final CollectProducer collectCountP = new CollectProducer(agg, collectCountID, allWorkers[0]);

    final CollectConsumer collectCountC =
        new CollectConsumer(collectCountP.getSchema(), collectCountID, allWorkers);

    final Aggregate aggSumCount =
        new Aggregate(collectCountC, new SingleColumnAggregatorFactory(0, AggregationOp.SUM));

    final CollectProducer sendToMaster = new CollectProducer(aggSumCount, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    result.put(
        allWorkers[0],
        new RootOperator[] {sendToMaster, shuffleArticlesP, shuffleSwrcPagesP, collectCountP});

    for (int i = 1; i < allWorkers.length; i++) {
      result.put(
          allWorkers[i], new RootOperator[] {shuffleArticlesP, shuffleSwrcPagesP, collectCountP});
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
}
