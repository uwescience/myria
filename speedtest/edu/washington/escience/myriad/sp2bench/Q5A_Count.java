package edu.washington.escience.myriad.sp2bench;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
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
import edu.washington.escience.myriad.parallel.WholeTupleHashPartitionFunction;

public class Q5A_Count implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE);
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

    final SingleFieldHashPartitionFunction pfOn0 = new SingleFieldHashPartitionFunction(allWorkers.length);
    pfOn0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final SingleFieldHashPartitionFunction pfOn1 = new SingleFieldHashPartitionFunction(allWorkers.length);
    pfOn1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);

    final SQLiteQueryScan allArticles =
        new SQLiteQueryScan(
            "select t.subject from Triples t, Dictionary dtype, Dictionary darticle where t.predicate=dtype.id and t.object=darticle.id and darticle.val='bench:Article' and dtype.val='rdf:type';",
            Schemas.subjectSchema);
    // schema: (articleId long), card:971330

    final ShuffleProducer shuffleArticlesP = new ShuffleProducer(allArticles, allArticlesShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleArticlesC =
        new ShuffleConsumer(shuffleArticlesP.getSchema(), allArticlesShuffleID, allWorkers);
    // schema: (articleId long)

    final SQLiteQueryScan allHasCreator =
        new SQLiteQueryScan(
            "select t.subject, t.object from Triples t, Dictionary dtype where t.predicate=dtype.id and dtype.val='dc:creator';",
            Schemas.subjectObjectSchema);
    // schema: (createdObjID long, creatorID long), card:10626906

    final ShuffleProducer shuffleCreatorsP =
        new ShuffleProducer(allHasCreator, allCreatorsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleCreatorsC =
        new ShuffleConsumer(shuffleCreatorsP.getSchema(), allCreatorsShuffleID, allWorkers);
    // schema: (createdObjID long, creatorID long)

    final LocalJoin joinArticleCreator =
        new LocalJoin(shuffleArticlesC, shuffleCreatorsC, new int[] { 0 }, new int[] { 0 });
    // schema: (articleId long, articleId long, creatorID long)

    final Project projArticleCreatorsID = new Project(new int[] { 2 }, joinArticleCreator);
    // schema: (articleAuthorIDs long)

    final DupElim deArticleAuthors = new DupElim(projArticleCreatorsID); // local dupelim
    // schema: (articleAuthorIDs long)

    final ShuffleProducer shuffleArticleCreatorsP =
        new ShuffleProducer(deArticleAuthors, articleCreatorsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleArticleCreatorsC =
        new ShuffleConsumer(shuffleArticleCreatorsP.getSchema(), articleCreatorsShuffleID, allWorkers);
    // schema: (articleAuthorIDs long)

    final DupElim deArticleAuthorsGlobal = new DupElim(shuffleArticleCreatorsC); // local dupelim
    // schema: (articleAuthorIDs long)

    final SQLiteQueryScan allInProceedings =
        new SQLiteQueryScan(
            "select t.subject from Triples t, Dictionary dtype, Dictionary dproceedings where t.predicate=dtype.id and t.object=dproceedings.id and dproceedings.val='bench:Inproceedings' and dtype.val='rdf:type';",
            Schemas.subjectSchema);
    // schema: (proceedingId long), card:2916364

    final ShuffleProducer shuffleProceedingsP =
        new ShuffleProducer(allInProceedings, allProceedingsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleProceedingsC =
        new ShuffleConsumer(shuffleProceedingsP.getSchema(), allProceedingsShuffleID, allWorkers);
    // schema: (proceedingId long)

    final SQLiteQueryScan allHasCreator2 =
        new SQLiteQueryScan(
            "select t.subject, t.object from Triples t, Dictionary dtype where t.predicate=dtype.id and dtype.val='dc:creator';",
            Schemas.subjectObjectSchema);
    // schema: (createdObjectID long, creatorId long), card: 10626906

    final ShuffleProducer shuffleCreators2P =
        new ShuffleProducer(allHasCreator2, allCreators2ShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleCreators2C =
        new ShuffleConsumer(shuffleCreators2P.getSchema(), allCreators2ShuffleID, allWorkers);

    final LocalJoin joinProceedingsCreator =
        new LocalJoin(shuffleProceedingsC, shuffleCreators2C, new int[] { 0 }, new int[] { 0 });
    // schema: (proceedingId long, proceedingId long, creatorID long)

    final Project projProceedingsID = new Project(new int[] { 2 }, joinProceedingsCreator);
    // schema: (proceedingAuthorID long)

    final DupElim deProceedingAuthors = new DupElim(projProceedingsID); // local dupelim

    final ShuffleProducer shuffleProceedingsCreatorsP =
        new ShuffleProducer(deProceedingAuthors, proceedingsCreatorsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleProceedingsCreatorsC =
        new ShuffleConsumer(shuffleProceedingsCreatorsP.getSchema(), proceedingsCreatorsShuffleID, allWorkers);
    // schema: (proceedingAuthorID long)

    final DupElim deProceedingAuthorsGlobal = new DupElim(shuffleProceedingsCreatorsC); // local dupelim

    final LocalJoin articleProceedingsCreatorJoin =
        new LocalJoin(deArticleAuthorsGlobal, deProceedingAuthorsGlobal, new int[] { 0 }, new int[] { 0 });
    // schema: (articleProceedingAuthorID long, articleProceedingAuthorID long)

    final SQLiteQueryScan allFOAF2 =
        new SQLiteQueryScan(
            "select t.subject,t.object from Triples t, Dictionary dtype where t.predicate=dtype.id and dtype.val='foaf:name';",
            Schemas.subjectObjectSchema);
    // schema: (foafSubjID long, foafObjID long)

    final ShuffleProducer shuffleFOAF2P = new ShuffleProducer(allFOAF2, allFOAF2ShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleFOAF2C = new ShuffleConsumer(shuffleFOAF2P.getSchema(), allFOAF2ShuffleID, allWorkers);
    // schema: (foafSubjID long, foafObjID long)

    final LocalJoin articleProceedingsCreatorFOAFJoin =
        new LocalJoin(articleProceedingsCreatorJoin, shuffleFOAF2C, new int[] { 1 }, new int[] { 0 });
    // schema: (articleProceedingAuthorID long, articleProceedingAuthorID long, foafNameID long)

    final Project finalProject = new Project(new int[] { 1, 2 }, articleProceedingsCreatorFOAFJoin);
    // schema: (articleProceedingAuthorID long, foafNameID long)

    final ShuffleProducer forDupElimShuffleP =
        new ShuffleProducer(finalProject, forDupElimShuffleID, allWorkers, new WholeTupleHashPartitionFunction(
            allWorkers.length));
    final ShuffleConsumer forDupElimShuffleC =
        new ShuffleConsumer(forDupElimShuffleP.getSchema(), forDupElimShuffleID, allWorkers);

    final DupElim dupElim = new DupElim(forDupElimShuffleC);

    final Aggregate agg = new Aggregate(dupElim, new int[] { 0 }, new int[] { Aggregator.AGG_OP_COUNT });

    final CollectProducer collectCountP = new CollectProducer(agg, collectCountID, allWorkers[0]);

    final CollectConsumer collectCountC = new CollectConsumer(collectCountP.getSchema(), collectCountID, allWorkers);

    final Aggregate aggSumCount = new Aggregate(collectCountC, new int[] { 0 }, new int[] { Aggregator.AGG_OP_SUM });

    final CollectProducer sendToMaster = new CollectProducer(aggSumCount, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    result.put(allWorkers[0], new RootOperator[] {
        sendToMaster, shuffleArticlesP, collectCountP, shuffleCreatorsP, shuffleArticleCreatorsP, shuffleProceedingsP,
        shuffleCreators2P, shuffleProceedingsCreatorsP, shuffleFOAF2P, forDupElimShuffleP });

    for (int i = 1; i < allWorkers.length; i++) {
      result.put(allWorkers[i], new RootOperator[] {
          shuffleArticlesP, collectCountP, shuffleCreatorsP, shuffleArticleCreatorsP, shuffleProceedingsP,
          shuffleCreators2P, shuffleProceedingsCreatorsP, shuffleFOAF2P, forDupElimShuffleP });
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

  public static void main(String[] args) throws Exception {
    System.out.println(new Q5A_Count().getWorkerPlan(new int[] { 0, 1, 2, 3, 4 }).get(0)[0]);
  }
}
