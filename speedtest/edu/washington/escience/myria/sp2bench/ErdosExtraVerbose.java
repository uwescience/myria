package edu.washington.escience.myria.sp2bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.LocalJoin;
import edu.washington.escience.myria.operator.Project;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.Producer;
import edu.washington.escience.myria.parallel.ShuffleConsumer;
import edu.washington.escience.myria.parallel.ShuffleProducer;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;

public class ErdosExtraVerbose {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("names");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final static ImmutableList<Type> singleStringTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> singleStringColumnNames = ImmutableList.of("string");
  final static Schema singleStringSchema = new Schema(singleStringTypes, singleStringColumnNames);

  final static ImmutableList<Type> twoStringsTypes = ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE);
  final static ImmutableList<String> twoStringsColumnNames = ImmutableList.of("string1", "string2");
  final static Schema twoStringsSchema = new Schema(twoStringsTypes, twoStringsColumnNames);

  final static ExchangePairID sendToMasterID = ExchangePairID.newID();

  public static DupElim erdosOne(int[] allWorkers, ArrayList<Producer> producers) throws DbException {
    final SingleFieldHashPartitionFunction pfOn0 = new SingleFieldHashPartitionFunction(allWorkers.length);
    final SingleFieldHashPartitionFunction pfOn1 = new SingleFieldHashPartitionFunction(allWorkers.length);
    final SingleFieldHashPartitionFunction pfOn2 = new SingleFieldHashPartitionFunction(allWorkers.length);
    pfOn0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    pfOn1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);
    pfOn2.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 2);

    final ExchangePairID paulErdoesPubsShuffleID = ExchangePairID.newID();
    final ExchangePairID coAuthorShuffleID = ExchangePairID.newID();
    final ExchangePairID allPubsShuffleID = ExchangePairID.newID();

    final DbQueryScan paulErdoesPubs = new DbQueryScan(//
        "select distinct pubName.val " + //
            "from Triples pubs " + //
            "join Dictionary pe on pubs.object=pe.id " + //
            "join Dictionary creator on pubs.predicate=creator.id " + //
            "join Dictionary pubName on pubs.subject=pubName.id " + //
            "where pe.val='<http://localhost/persons/Paul_Erdoes>' " + //
            "and creator.val='dc:creator'",//
        singleStringSchema);
    // schema: (pubName string)

    final ShuffleProducer paulErdoesPubsShuffleP =
        new ShuffleProducer(paulErdoesPubs, paulErdoesPubsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer paulErdoesPubsShuffleC =
        new ShuffleConsumer(paulErdoesPubsShuffleP.getSchema(), paulErdoesPubsShuffleID, allWorkers);
    // schema: (pubName string)

    final DbQueryScan allPubs = new DbQueryScan(//
        "select pubName.val,authorName.val " + //
            "from Triples authors " + //
            "join Dictionary creator on authors.predicate=creator.id " + //
            "join Dictionary pubName on authors.subject=pubName.id " + //
            "join Dictionary authorName on authors.object=authorName.id " + //
            "where creator.val='dc:creator' ", //
        twoStringsSchema);
    // schema: (pubName string, authorName string)

    final ShuffleProducer allPubsShuffleP = new ShuffleProducer(allPubs, allPubsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer allPubsShuffleC =
        new ShuffleConsumer(allPubsShuffleP.getSchema(), allPubsShuffleID, allWorkers);
    // schema: (pubName string, authorName string)

    final LocalJoin joinCoAuthors =
        new LocalJoin(paulErdoesPubsShuffleC, allPubsShuffleC, new int[] { 0 }, new int[] { 0 });
    // schema: (pubName string, pubName String, authorName string)

    // final Project projCoAuthorID = new Project(new Integer[] { 2 }, joinCoAuthors);
    // schema: (authorName String)
    // final DupElim localDECoAuthorID = new DupElim(projCoAuthorID); // local dupelim
    // schema: (authorName String)

    final ShuffleProducer coAuthorShuffleP = new ShuffleProducer(joinCoAuthors, coAuthorShuffleID, allWorkers, pfOn2);
    final ShuffleConsumer coAuthorShuffleC =
        new ShuffleConsumer(coAuthorShuffleP.getSchema(), coAuthorShuffleID, allWorkers);
    // schema: (pubName string, pubName String, authorName string)

    final Project projCoAuthorID = new Project(new int[] { 2 }, coAuthorShuffleC);
    // schema: (authorName string)

    producers.add(coAuthorShuffleP);
    producers.add(allPubsShuffleP);
    producers.add(paulErdoesPubsShuffleP);
    // schema: (authorName string)
    return new DupElim(projCoAuthorID);
  }

  public static DupElim erdosN(DupElim erdosNMinus1, int[] allWorkers, ArrayList<Producer> producers)
      throws DbException {

    final SingleFieldHashPartitionFunction pfOn0 = new SingleFieldHashPartitionFunction(allWorkers.length);
    final SingleFieldHashPartitionFunction pfOn1 = new SingleFieldHashPartitionFunction(allWorkers.length);
    final SingleFieldHashPartitionFunction pfOn2 = new SingleFieldHashPartitionFunction(allWorkers.length);
    pfOn0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);
    pfOn1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);
    pfOn2.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 2);

    final DbQueryScan allPubs2 = new DbQueryScan(//
        "select pubName.val, authorName.val " + //
            "from Triples authors " + //
            "join Dictionary creator on authors.predicate=creator.id " + //
            "join Dictionary pubName on authors.subject=pubName.id " + //
            "join Dictionary authorName on authors.object=authorName.id " + //
            "where creator.val='dc:creator' ", //
        twoStringsSchema);
    // schema: (pubName string, authorName String)

    ExchangePairID allPubsShuffleByAuthorID = ExchangePairID.newID();
    final ShuffleProducer allPubsShuffleByAuthorP =
        new ShuffleProducer(allPubs2, allPubsShuffleByAuthorID, allWorkers, pfOn1);
    final ShuffleConsumer allPubsShuffleByAuthorC =
        new ShuffleConsumer(allPubsShuffleByAuthorP.getSchema(), allPubsShuffleByAuthorID, allWorkers);
    // schema: (pubName string, authorName String)

    final LocalJoin joinCoAuthorPubs =
        new LocalJoin(erdosNMinus1, allPubsShuffleByAuthorC, new int[] { 0 }, new int[] { 1 });
    // schema: (authorName string, pubName string, authorName String)

    // schema: (pubName string)

    // final DupElim coAuthorPubsLocalDE = new DupElim(projCoAuthorPubsID); // local dupelim
    // schema: (pubName string)

    ExchangePairID coAuthorPubsShuffleID = ExchangePairID.newID();
    final ShuffleProducer coAuthorPubsShuffleP =
        new ShuffleProducer(joinCoAuthorPubs, coAuthorPubsShuffleID, allWorkers, pfOn1);
    final ShuffleConsumer coAuthorPubsShuffleC =
        new ShuffleConsumer(coAuthorPubsShuffleP.getSchema(), coAuthorPubsShuffleID, allWorkers);
    // schema: (authorName string, pubName string, authorName String)

    final Project projCoAuthorPubsID = new Project(new int[] { 1 }, coAuthorPubsShuffleC);
    // schema: (pubName string)

    final DupElim coAuthorPubsGlobalDE = new DupElim(projCoAuthorPubsID); // local dupelim
    // schema: (pubName string)

    final DbQueryScan allPubsAuthorNames = new DbQueryScan(//
        "select pubName.val ,names.val as authorName " + //
            "from Triples authors " + //
            "join Dictionary creator on authors.predicate=creator.id " + //
            "join Dictionary names on authors.object=names.id " + //
            "join Dictionary pubName on authors.subject=pubName.id " + //
            "where creator.val='dc:creator' ", //
        twoStringsSchema);
    // schema: (pubName string, authorName string)

    ExchangePairID coAuthorNamesPubsShuffleID = ExchangePairID.newID();
    final ShuffleProducer coAuthorNamesPubsShuffleP =
        new ShuffleProducer(allPubsAuthorNames, coAuthorNamesPubsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer coAuthorNamesPubsShuffleC =
        new ShuffleConsumer(coAuthorNamesPubsShuffleP.getSchema(), coAuthorNamesPubsShuffleID, allWorkers);
    // schema: (pubName string, authorName string)

    final LocalJoin joinCoCoAuthorPubs =
        new LocalJoin(coAuthorPubsGlobalDE, coAuthorNamesPubsShuffleC, new int[] { 0 }, new int[] { 0 });
    // schema: (pubName string, pubName string, authorName string)

    // schema: (authorName string)

    // final DupElim coCoAuthorNameLocalDE = new DupElim(projCoCoAuthorName); // local dupelim
    // schema: (authorName string)

    ExchangePairID coCoAuthorNameShuffleID = ExchangePairID.newID();
    final ShuffleProducer coCoAuthorNameShuffleP =
        new ShuffleProducer(joinCoCoAuthorPubs, coCoAuthorNameShuffleID, allWorkers, pfOn2);
    final ShuffleConsumer coCoAuthorNameShuffleC =
        new ShuffleConsumer(coCoAuthorNameShuffleP.getSchema(), coCoAuthorNameShuffleID, allWorkers);
    // schema: (pubName string, pubName string, authorName string)

    final Project projCoCoAuthorName = new Project(new int[] { 2 }, coCoAuthorNameShuffleC);

    producers.add(coCoAuthorNameShuffleP);
    producers.add(coAuthorNamesPubsShuffleP);
    producers.add(coAuthorPubsShuffleP);
    producers.add(allPubsShuffleByAuthorP);
    return new DupElim(projCoCoAuthorName); // local dupelim
    // schema: (authorName string)
  }

  public static DupElim erdosN(int n, int[] allWorkers, ArrayList<Producer> producers) throws DbException {
    DupElim erdos1 = erdosOne(allWorkers, producers);
    if (n <= 1) {
      return erdos1;
    } else {
      DupElim erdosNMinus1 = erdos1;
      for (int i = 1; i < n; i++) {
        erdosNMinus1 = erdosN(erdosNMinus1, allWorkers, producers);
      }
      return erdosNMinus1;
    }
  }

  public static Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers, DupElim de, ArrayList<Producer> producers)
      throws Exception {

    final CollectProducer sendToMaster = new CollectProducer(de, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    producers.add(sendToMaster);
    for (int allWorker : allWorkers) {
      result.put(allWorker, producers.toArray(new RootOperator[] {}));
    }
    return result;
  }

  public static RootOperator getMasterPlan(int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception {
    // TODO Auto-generated method stub
    final CollectConsumer serverCollect = new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    return serverPlan;
  }

}
