package edu.washington.escience.myria.sp2bench;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Applys;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
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
import edu.washington.escience.myria.operator.network.partition.WholeTupleHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class Q5A_Count implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes =
      ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("person", "name");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final ExchangePairID allArticlesShuffleID = ExchangePairID.newID();
    final ExchangePairID allProceedingsShuffleID = ExchangePairID.newID();
    final ExchangePairID allCreatorsShuffleID = ExchangePairID.newID();
    final ExchangePairID allCreators2ShuffleID = ExchangePairID.newID();
    final ExchangePairID allFOAF2ShuffleID = ExchangePairID.newID();
    final ExchangePairID articleCreatorsShuffleID = ExchangePairID.newID();
    final ExchangePairID proceedingsCreatorsShuffleID = ExchangePairID.newID();
    final ExchangePairID forDupElimShuffleID = ExchangePairID.newID();
    final ExchangePairID collectCountID = ExchangePairID.newID();

    final SingleFieldHashPartitionFunction pfOn0 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 0);

    final DbQueryScan allArticles =
        new DbQueryScan(
            "select t.subject from Triples t, Dictionary dtype, Dictionary darticle where t.predicate=dtype.id and t.object=darticle.id and darticle.val='bench:Article' and dtype.val='rdf:type';",
            Schemas.subjectSchema);
    // schema: (articleId long), card:971330

    final GenericShuffleProducer shuffleArticlesP =
        new GenericShuffleProducer(allArticles, allArticlesShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleArticlesC =
        new GenericShuffleConsumer(shuffleArticlesP.getSchema(), allArticlesShuffleID, allWorkers);
    // schema: (articleId long)

    final DbQueryScan allHasCreator =
        new DbQueryScan(
            "select t.subject, t.object from Triples t, Dictionary dtype where t.predicate=dtype.id and dtype.val='dc:creator';",
            Schemas.subjectObjectSchema);
    // schema: (createdObjID long, creatorID long), card:10626906

    final GenericShuffleProducer shuffleCreatorsP =
        new GenericShuffleProducer(allHasCreator, allCreatorsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleCreatorsC =
        new GenericShuffleConsumer(shuffleCreatorsP.getSchema(), allCreatorsShuffleID, allWorkers);
    // schema: (createdObjID long, creatorID long)

    final SymmetricHashJoin joinArticleCreator =
        new SymmetricHashJoin(shuffleArticlesC, shuffleCreatorsC, new int[] {0}, new int[] {0});
    // schema: (articleId long, articleId long, creatorID long)

    final Apply projArticleCreatorsID = Applys.columnSelect(joinArticleCreator, 2);
    // schema: (articleAuthorIDs long)

    final StreamingStateWrapper deArticleAuthors =
        new StreamingStateWrapper(projArticleCreatorsID, new DupElim());
    // schema: (articleAuthorIDs long)

    final GenericShuffleProducer shuffleArticleCreatorsP =
        new GenericShuffleProducer(deArticleAuthors, articleCreatorsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleArticleCreatorsC =
        new GenericShuffleConsumer(
            shuffleArticleCreatorsP.getSchema(), articleCreatorsShuffleID, allWorkers);
    // schema: (articleAuthorIDs long)

    final StreamingStateWrapper deArticleAuthorsGlobal =
        new StreamingStateWrapper(shuffleArticleCreatorsC, new DupElim()); // local
    // dupelim
    // schema: (articleAuthorIDs long)

    final DbQueryScan allInProceedings =
        new DbQueryScan(
            "select t.subject from Triples t, Dictionary dtype, Dictionary dproceedings where t.predicate=dtype.id and t.object=dproceedings.id and dproceedings.val='bench:Inproceedings' and dtype.val='rdf:type';",
            Schemas.subjectSchema);
    // schema: (proceedingId long), card:2916364

    final GenericShuffleProducer shuffleProceedingsP =
        new GenericShuffleProducer(allInProceedings, allProceedingsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleProceedingsC =
        new GenericShuffleConsumer(
            shuffleProceedingsP.getSchema(), allProceedingsShuffleID, allWorkers);
    // schema: (proceedingId long)

    final DbQueryScan allHasCreator2 =
        new DbQueryScan(
            "select t.subject, t.object from Triples t, Dictionary dtype where t.predicate=dtype.id and dtype.val='dc:creator';",
            Schemas.subjectObjectSchema);
    // schema: (createdObjectID long, creatorId long), card: 10626906

    final GenericShuffleProducer shuffleCreators2P =
        new GenericShuffleProducer(allHasCreator2, allCreators2ShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleCreators2C =
        new GenericShuffleConsumer(
            shuffleCreators2P.getSchema(), allCreators2ShuffleID, allWorkers);

    final SymmetricHashJoin joinProceedingsCreator =
        new SymmetricHashJoin(shuffleProceedingsC, shuffleCreators2C, new int[] {0}, new int[] {0});
    // schema: (proceedingId long, proceedingId long, creatorID long)

    final Apply projProceedingsID = Applys.columnSelect(joinProceedingsCreator, 2);
    // schema: (proceedingAuthorID long)

    final StreamingStateWrapper deProceedingAuthors =
        new StreamingStateWrapper(projProceedingsID, new DupElim());

    final GenericShuffleProducer shuffleProceedingsCreatorsP =
        new GenericShuffleProducer(
            deProceedingAuthors, proceedingsCreatorsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleProceedingsCreatorsC =
        new GenericShuffleConsumer(
            shuffleProceedingsCreatorsP.getSchema(), proceedingsCreatorsShuffleID, allWorkers);
    // schema: (proceedingAuthorID long)

    final StreamingStateWrapper deProceedingAuthorsGlobal =
        new StreamingStateWrapper(shuffleProceedingsCreatorsC, new DupElim());

    final SymmetricHashJoin articleProceedingsCreatorJoin =
        new SymmetricHashJoin(
            deArticleAuthorsGlobal, deProceedingAuthorsGlobal, new int[] {0}, new int[] {0});
    // schema: (articleProceedingAuthorID long, articleProceedingAuthorID long)

    final DbQueryScan allFOAF2 =
        new DbQueryScan(
            "select t.subject,t.object from Triples t, Dictionary dtype where t.predicate=dtype.id and dtype.val='foaf:name';",
            Schemas.subjectObjectSchema);
    // schema: (foafSubjID long, foafObjID long)

    final GenericShuffleProducer shuffleFOAF2P =
        new GenericShuffleProducer(allFOAF2, allFOAF2ShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleFOAF2C =
        new GenericShuffleConsumer(shuffleFOAF2P.getSchema(), allFOAF2ShuffleID, allWorkers);
    // schema: (foafSubjID long, foafObjID long)

    final SymmetricHashJoin articleProceedingsCreatorFOAFJoin =
        new SymmetricHashJoin(
            articleProceedingsCreatorJoin, shuffleFOAF2C, new int[] {1}, new int[] {0});
    // schema: (articleProceedingAuthorID long, articleProceedingAuthorID long, foafNameID long)

    final Apply finalColSelect = Applys.columnSelect(articleProceedingsCreatorFOAFJoin, 1, 2);
    // schema: (articleProceedingAuthorID long, foafNameID long)

    final GenericShuffleProducer forDupElimShuffleP =
        new GenericShuffleProducer(
            finalColSelect,
            forDupElimShuffleID,
            allWorkers,
            new WholeTupleHashPartitionFunction(allWorkers.length));
    final GenericShuffleConsumer forDupElimShuffleC =
        new GenericShuffleConsumer(forDupElimShuffleP.getSchema(), forDupElimShuffleID, allWorkers);

    final StreamingStateWrapper dupElim =
        new StreamingStateWrapper(forDupElimShuffleC, new DupElim());

    final Aggregate agg =
        new Aggregate(dupElim, new SingleColumnAggregatorFactory(0, AggregationOp.COUNT));

    final CollectProducer collectCountP = new CollectProducer(agg, collectCountID, allWorkers[0]);

    final CollectConsumer collectCountC =
        new CollectConsumer(collectCountP.getSchema(), collectCountID, allWorkers);

    final Aggregate aggSumCount =
        new Aggregate(collectCountC, new SingleColumnAggregatorFactory(0, AggregationOp.SUM));

    final CollectProducer sendToMaster = new CollectProducer(aggSumCount, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    result.put(
        allWorkers[0],
        new RootOperator[] {
          sendToMaster,
          shuffleArticlesP,
          collectCountP,
          shuffleCreatorsP,
          shuffleArticleCreatorsP,
          shuffleProceedingsP,
          shuffleCreators2P,
          shuffleProceedingsCreatorsP,
          shuffleFOAF2P,
          forDupElimShuffleP
        });

    for (int i = 1; i < allWorkers.length; i++) {
      result.put(
          allWorkers[i],
          new RootOperator[] {
            shuffleArticlesP,
            collectCountP,
            shuffleCreatorsP,
            shuffleArticleCreatorsP,
            shuffleProceedingsP,
            shuffleCreators2P,
            shuffleProceedingsCreatorsP,
            shuffleFOAF2P,
            forDupElimShuffleP
          });
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
    System.out.println(new Q5A_Count().getWorkerPlan(new int[] {0, 1, 2, 3, 4}).get(0)[0]);
  }
}
