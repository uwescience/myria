package edu.washington.escience.myria.sp2bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
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
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class ErdosExtraVerbose {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("names");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final static ImmutableList<Type> singleStringTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> singleStringColumnNames = ImmutableList.of("string");
  final static Schema singleStringSchema = new Schema(singleStringTypes, singleStringColumnNames);

  final static ImmutableList<Type> twoStringsTypes =
      ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE);
  final static ImmutableList<String> twoStringsColumnNames = ImmutableList.of("string1", "string2");
  final static Schema twoStringsSchema = new Schema(twoStringsTypes, twoStringsColumnNames);

  final static ExchangePairID sendToMasterID = ExchangePairID.newID();

  public static StreamingStateWrapper erdosOne(int[] allWorkers, ArrayList<Producer> producers)
      throws DbException {
    final SingleFieldHashPartitionFunction pfOn0 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 0);
    final SingleFieldHashPartitionFunction pfOn2 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 2);

    final ExchangePairID paulErdoesPubsShuffleID = ExchangePairID.newID();
    final ExchangePairID coAuthorShuffleID = ExchangePairID.newID();
    final ExchangePairID allPubsShuffleID = ExchangePairID.newID();

    final DbQueryScan paulErdoesPubs =
        new DbQueryScan( //
            "select distinct pubName.val "
                + //
                "from Triples pubs "
                + //
                "join Dictionary pe on pubs.object=pe.id "
                + //
                "join Dictionary creator on pubs.predicate=creator.id "
                + //
                "join Dictionary pubName on pubs.subject=pubName.id "
                + //
                "where pe.val='<http://localhost/persons/Paul_Erdoes>' "
                + //
                "and creator.val='dc:creator'", //
            singleStringSchema);
    // schema: (pubName string)

    final GenericShuffleProducer paulErdoesPubsShuffleP =
        new GenericShuffleProducer(paulErdoesPubs, paulErdoesPubsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer paulErdoesPubsShuffleC =
        new GenericShuffleConsumer(
            paulErdoesPubsShuffleP.getSchema(), paulErdoesPubsShuffleID, allWorkers);
    // schema: (pubName string)

    final DbQueryScan allPubs =
        new DbQueryScan( //
            "select pubName.val,authorName.val "
                + //
                "from Triples authors "
                + //
                "join Dictionary creator on authors.predicate=creator.id "
                + //
                "join Dictionary pubName on authors.subject=pubName.id "
                + //
                "join Dictionary authorName on authors.object=authorName.id "
                + //
                "where creator.val='dc:creator' ", //
            twoStringsSchema);
    // schema: (pubName string, authorName string)

    final GenericShuffleProducer allPubsShuffleP =
        new GenericShuffleProducer(allPubs, allPubsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer allPubsShuffleC =
        new GenericShuffleConsumer(allPubsShuffleP.getSchema(), allPubsShuffleID, allWorkers);
    // schema: (pubName string, authorName string)

    final SymmetricHashJoin joinCoAuthors =
        new SymmetricHashJoin(
            paulErdoesPubsShuffleC, allPubsShuffleC, new int[] {0}, new int[] {0});
    // schema: (pubName string, pubName String, authorName string)

    // final DupElim localDECoAuthorID = new DupElim(projCoAuthorID); // local dupelim
    // schema: (authorName String)

    final GenericShuffleProducer coAuthorShuffleP =
        new GenericShuffleProducer(joinCoAuthors, coAuthorShuffleID, allWorkers, pfOn2);
    final GenericShuffleConsumer coAuthorShuffleC =
        new GenericShuffleConsumer(coAuthorShuffleP.getSchema(), coAuthorShuffleID, allWorkers);
    // schema: (pubName string, pubName String, authorName string)

    final Apply projCoAuthorID = Applys.columnSelect(coAuthorShuffleC, 2);
    // schema: (authorName string)

    producers.add(coAuthorShuffleP);
    producers.add(allPubsShuffleP);
    producers.add(paulErdoesPubsShuffleP);
    // schema: (authorName string)
    return new StreamingStateWrapper(projCoAuthorID, new DupElim());
  }

  public static StreamingStateWrapper erdosN(
      StreamingStateWrapper erdosNMinus1, int[] allWorkers, ArrayList<Producer> producers)
      throws DbException {

    final SingleFieldHashPartitionFunction pfOn0 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 0);
    final SingleFieldHashPartitionFunction pfOn1 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 1);
    final SingleFieldHashPartitionFunction pfOn2 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 2);

    final DbQueryScan allPubs2 =
        new DbQueryScan( //
            "select pubName.val, authorName.val "
                + //
                "from Triples authors "
                + //
                "join Dictionary creator on authors.predicate=creator.id "
                + //
                "join Dictionary pubName on authors.subject=pubName.id "
                + //
                "join Dictionary authorName on authors.object=authorName.id "
                + //
                "where creator.val='dc:creator' ", //
            twoStringsSchema);
    // schema: (pubName string, authorName String)

    ExchangePairID allPubsShuffleByAuthorID = ExchangePairID.newID();
    final GenericShuffleProducer allPubsShuffleByAuthorP =
        new GenericShuffleProducer(allPubs2, allPubsShuffleByAuthorID, allWorkers, pfOn1);
    final GenericShuffleConsumer allPubsShuffleByAuthorC =
        new GenericShuffleConsumer(
            allPubsShuffleByAuthorP.getSchema(), allPubsShuffleByAuthorID, allWorkers);
    // schema: (pubName string, authorName String)

    final SymmetricHashJoin joinCoAuthorPubs =
        new SymmetricHashJoin(erdosNMinus1, allPubsShuffleByAuthorC, new int[] {0}, new int[] {1});
    // schema: (authorName string, pubName string, authorName String)

    // schema: (pubName string)

    // final DupElim coAuthorPubsLocalDE = new DupElim(projCoAuthorPubsID); // local dupelim
    // schema: (pubName string)

    ExchangePairID coAuthorPubsShuffleID = ExchangePairID.newID();
    final GenericShuffleProducer coAuthorPubsShuffleP =
        new GenericShuffleProducer(joinCoAuthorPubs, coAuthorPubsShuffleID, allWorkers, pfOn1);
    final GenericShuffleConsumer coAuthorPubsShuffleC =
        new GenericShuffleConsumer(
            coAuthorPubsShuffleP.getSchema(), coAuthorPubsShuffleID, allWorkers);
    // schema: (authorName string, pubName string, authorName String)

    final Apply projCoAuthorPubsID = Applys.columnSelect(coAuthorPubsShuffleC, 1);
    // schema: (pubName string)

    final StreamingStateWrapper coAuthorPubsGlobalDE =
        new StreamingStateWrapper(projCoAuthorPubsID, new DupElim());
    // schema: (pubName string)

    final DbQueryScan allPubsAuthorNames =
        new DbQueryScan( //
            "select pubName.val ,names.val as authorName "
                + //
                "from Triples authors "
                + //
                "join Dictionary creator on authors.predicate=creator.id "
                + //
                "join Dictionary names on authors.object=names.id "
                + //
                "join Dictionary pubName on authors.subject=pubName.id "
                + //
                "where creator.val='dc:creator' ", //
            twoStringsSchema);
    // schema: (pubName string, authorName string)

    ExchangePairID coAuthorNamesPubsShuffleID = ExchangePairID.newID();
    final GenericShuffleProducer coAuthorNamesPubsShuffleP =
        new GenericShuffleProducer(
            allPubsAuthorNames, coAuthorNamesPubsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer coAuthorNamesPubsShuffleC =
        new GenericShuffleConsumer(
            coAuthorNamesPubsShuffleP.getSchema(), coAuthorNamesPubsShuffleID, allWorkers);
    // schema: (pubName string, authorName string)

    final SymmetricHashJoin joinCoCoAuthorPubs =
        new SymmetricHashJoin(
            coAuthorPubsGlobalDE, coAuthorNamesPubsShuffleC, new int[] {0}, new int[] {0});
    // schema: (pubName string, pubName string, authorName string)

    // schema: (authorName string)

    // final DupElim coCoAuthorNameLocalDE = new DupElim(projCoCoAuthorName); // local dupelim
    // schema: (authorName string)

    ExchangePairID coCoAuthorNameShuffleID = ExchangePairID.newID();
    final GenericShuffleProducer coCoAuthorNameShuffleP =
        new GenericShuffleProducer(joinCoCoAuthorPubs, coCoAuthorNameShuffleID, allWorkers, pfOn2);
    final GenericShuffleConsumer coCoAuthorNameShuffleC =
        new GenericShuffleConsumer(
            coCoAuthorNameShuffleP.getSchema(), coCoAuthorNameShuffleID, allWorkers);
    // schema: (pubName string, pubName string, authorName string)

    final Apply projCoCoAuthorName = Applys.columnSelect(coCoAuthorNameShuffleC, 2);

    producers.add(coCoAuthorNameShuffleP);
    producers.add(coAuthorNamesPubsShuffleP);
    producers.add(coAuthorPubsShuffleP);
    producers.add(allPubsShuffleByAuthorP);
    return new StreamingStateWrapper(projCoCoAuthorName, new DupElim()); // local dupelim
    // schema: (authorName string)
  }

  public static StreamingStateWrapper erdosN(int n, int[] allWorkers, ArrayList<Producer> producers)
      throws DbException {
    StreamingStateWrapper erdos1 = erdosOne(allWorkers, producers);
    if (n <= 1) {
      return erdos1;
    } else {
      StreamingStateWrapper erdosNMinus1 = erdos1;
      for (int i = 1; i < n; i++) {
        erdosNMinus1 = erdosN(erdosNMinus1, allWorkers, producers);
      }
      return erdosNMinus1;
    }
  }

  public static Map<Integer, RootOperator[]> getWorkerPlan(
      int[] allWorkers, StreamingStateWrapper de, ArrayList<Producer> producers) throws Exception {

    final CollectProducer sendToMaster = new CollectProducer(de, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    producers.add(sendToMaster);
    for (int allWorker : allWorkers) {
      result.put(allWorker, producers.toArray(new RootOperator[] {}));
    }
    return result;
  }

  public static RootOperator getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception {
    // TODO Auto-generated method stub
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    return serverPlan;
  }
}
