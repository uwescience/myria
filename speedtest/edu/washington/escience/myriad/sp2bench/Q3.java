package edu.washington.escience.myriad.sp2bench;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.DbQueryScan;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.operator.agg.Aggregate;
import edu.washington.escience.myriad.operator.agg.Aggregator;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

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

    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(allWorkers.length);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final ShuffleProducer shuffleArticlesP = new ShuffleProducer(allArticles, allArticlesShuffleID, allWorkers, pf);
    final ShuffleConsumer shuffleArticlesC =
        new ShuffleConsumer(shuffleArticlesP.getSchema(), allArticlesShuffleID, allWorkers);

    final ShuffleProducer shuffleSwrcPagesP =
        new ShuffleProducer(allWithSwrcPages, allSwrcPagesShuffleID, allWorkers, pf);
    final ShuffleConsumer shuffleSwrcPagesC =
        new ShuffleConsumer(shuffleSwrcPagesP.getSchema(), allSwrcPagesShuffleID, allWorkers);

    final LocalJoin joinArticleSwrcPages =
        new LocalJoin(shuffleArticlesC, shuffleSwrcPagesC, new int[] { 0 }, new int[] { 0 });

    final Aggregate agg = new Aggregate(joinArticleSwrcPages, new int[] { 0 }, new int[] { Aggregator.AGG_OP_COUNT });

    final CollectProducer collectCountP = new CollectProducer(agg, collectCountID, allWorkers[0]);

    final CollectConsumer collectCountC = new CollectConsumer(collectCountP.getSchema(), collectCountID, allWorkers);

    final Aggregate aggSumCount = new Aggregate(collectCountC, new int[] { 0 }, new int[] { Aggregator.AGG_OP_SUM });

    final CollectProducer sendToMaster = new CollectProducer(aggSumCount, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    result.put(allWorkers[0], new RootOperator[] { sendToMaster, shuffleArticlesP, shuffleSwrcPagesP, collectCountP });

    for (int i = 1; i < allWorkers.length; i++) {
      result.put(allWorkers[i], new RootOperator[] { shuffleArticlesP, shuffleSwrcPagesP, collectCountP });
    }

    return result;
  }

  @Override
  public RootOperator getMasterPlan(int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, sendToMasterID, new int[] { allWorkers[0] });
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    return serverPlan;
  }
}
