package edu.washington.escience.myria.sp2bench;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SQLiteSetFilter;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class Erdos {

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

    final ExchangePairID paulErdoesPubsShuffleID = ExchangePairID.newID();
    final ExchangePairID coAuthorShuffleID = ExchangePairID.newID();
    final ExchangePairID allPubsShuffleID = ExchangePairID.newID();

    final DbQueryScan paulErdoesPubs =
        new DbQueryScan( //
            "select distinct pubs.subject "
                + //
                "from Triples pubs "
                + //
                "join Dictionary pe on pubs.object=pe.id "
                + //
                "join Dictionary creator on pubs.predicate=creator.id "
                + //
                "where pe.val='<http://localhost/persons/Paul_Erdoes>' "
                + //
                "and creator.val='dc:creator'", //
            Schemas.subjectSchema);
    // schema: (pubId long)

    final GenericShuffleProducer paulErdoesPubsShuffleP =
        new GenericShuffleProducer(paulErdoesPubs, paulErdoesPubsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer paulErdoesPubsShuffleC =
        new GenericShuffleConsumer(
            paulErdoesPubsShuffleP.getSchema(), paulErdoesPubsShuffleID, allWorkers);
    // schema: (pubId long)

    final DbQueryScan allPubs =
        new DbQueryScan( //
            "select authors.subject as pubId,authors.object as authorId "
                + //
                "from Triples authors "
                + //
                "join Dictionary creator on authors.predicate=creator.id "
                + //
                "where creator.val='dc:creator' ", //
            Schemas.subjectObjectSchema);
    // schema: (pubId long, authorId long)

    final GenericShuffleProducer allPubsShuffleP =
        new GenericShuffleProducer(allPubs, allPubsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer allPubsShuffleC =
        new GenericShuffleConsumer(allPubsShuffleP.getSchema(), allPubsShuffleID, allWorkers);
    // schema: (pubId long, authorId long)

    final List<String> joinColumnNames = ImmutableList.of("pubId1", "pubId2", "authorId");
    final SymmetricHashJoin joinCoAuthors =
        new SymmetricHashJoin(
            joinColumnNames, paulErdoesPubsShuffleC, allPubsShuffleC, new int[] {0}, new int[] {0});
    // schema: (pubId long, pubId long, authorId long)

    final Apply projCoAuthorID = Applys.columnSelect(joinCoAuthors, 2);
    // schema: (authorId long)
    final StreamingStateWrapper localDECoAuthorID =
        new StreamingStateWrapper(projCoAuthorID, new DupElim());
    // schema: (authorId long)

    final GenericShuffleProducer coAuthorShuffleP =
        new GenericShuffleProducer(localDECoAuthorID, coAuthorShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer coAuthorShuffleC =
        new GenericShuffleConsumer(coAuthorShuffleP.getSchema(), coAuthorShuffleID, allWorkers);
    // schema: (authorId long)
    producers.add(coAuthorShuffleP);
    producers.add(allPubsShuffleP);
    producers.add(paulErdoesPubsShuffleP);
    // schema: (authorId long)
    return new StreamingStateWrapper(coAuthorShuffleC, new DupElim());
  }

  public static StreamingStateWrapper erdosN(
      StreamingStateWrapper erdosNMinus1, int[] allWorkers, ArrayList<Producer> producers)
      throws DbException {

    final SingleFieldHashPartitionFunction pfOn0 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 0);
    final SingleFieldHashPartitionFunction pfOn1 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 1);

    final DbQueryScan allPubs2 =
        new DbQueryScan( //
            "select authors.subject as pubId, authors.object as authorId "
                + //
                "from Triples authors "
                + //
                "join Dictionary creator on authors.predicate=creator.id "
                + //
                "where creator.val='dc:creator' ", //
            Schemas.subjectObjectSchema);
    // schema: (pubId long, authorId long)

    ExchangePairID allPubsShuffleByAuthorID = ExchangePairID.newID();
    final GenericShuffleProducer allPubsShuffleByAuthorP =
        new GenericShuffleProducer(allPubs2, allPubsShuffleByAuthorID, allWorkers, pfOn1);
    final GenericShuffleConsumer allPubsShuffleByAuthorC =
        new GenericShuffleConsumer(
            allPubsShuffleByAuthorP.getSchema(), allPubsShuffleByAuthorID, allWorkers);
    // schema: (pubId long, authorId long)

    final SymmetricHashJoin joinCoAuthorPubs =
        new SymmetricHashJoin(erdosNMinus1, allPubsShuffleByAuthorC, new int[] {0}, new int[] {1});
    // schema: (authorId long, pubId long, authorId long)

    final Apply projCoAuthorPubsID = Applys.columnSelect(joinCoAuthorPubs, 1);
    // schema: (pubId long)

    final StreamingStateWrapper coAuthorPubsLocalDE =
        new StreamingStateWrapper(projCoAuthorPubsID, new DupElim());
    // schema: (pubId long)

    ExchangePairID coAuthorPubsShuffleID = ExchangePairID.newID();
    final GenericShuffleProducer coAuthorPubsShuffleP =
        new GenericShuffleProducer(coAuthorPubsLocalDE, coAuthorPubsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer coAuthorPubsShuffleC =
        new GenericShuffleConsumer(
            coAuthorPubsShuffleP.getSchema(), coAuthorPubsShuffleID, allWorkers);
    // schema: (pubId long)

    final StreamingStateWrapper coAuthorPubsGlobalDE =
        new StreamingStateWrapper(coAuthorPubsShuffleC, new DupElim());
    // schema: (pubId long)

    final DbQueryScan allPubsAuthorNames =
        new DbQueryScan( //
            "select authors.subject as pubId,authors.object as authorId "
                + //
                "from Triples authors "
                + //
                "join Dictionary creator on authors.predicate=creator.id "
                + //
                "where creator.val='dc:creator' ", //
            Schemas.subjectObjectSchema);
    // schema: (pubId long, authorId long)

    ExchangePairID coAuthorNamesPubsShuffleID = ExchangePairID.newID();
    final GenericShuffleProducer coAuthorNamesPubsShuffleP =
        new GenericShuffleProducer(
            allPubsAuthorNames, coAuthorNamesPubsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer coAuthorNamesPubsShuffleC =
        new GenericShuffleConsumer(
            coAuthorNamesPubsShuffleP.getSchema(), coAuthorNamesPubsShuffleID, allWorkers);
    // schema: (pubId long, authorId long)

    final List<String> joinCoCoAuthorPubsNames = ImmutableList.of("subject1", "subject2", "object");
    final SymmetricHashJoin joinCoCoAuthorPubs =
        new SymmetricHashJoin(
            joinCoCoAuthorPubsNames,
            coAuthorPubsGlobalDE,
            coAuthorNamesPubsShuffleC,
            new int[] {0},
            new int[] {0});
    // schema: (pubId long, pubId long, authorId long)

    final Apply projCoCoAuthorName = Applys.columnSelect(joinCoCoAuthorPubs, 2);
    // schema: (authorId long)

    final StreamingStateWrapper coCoAuthorNameLocalDE =
        new StreamingStateWrapper(projCoCoAuthorName, new DupElim());
    // schema: (authorId long)

    ExchangePairID coCoAuthorNameShuffleID = ExchangePairID.newID();
    final GenericShuffleProducer coCoAuthorNameShuffleP =
        new GenericShuffleProducer(
            coCoAuthorNameLocalDE, coCoAuthorNameShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer coCoAuthorNameShuffleC =
        new GenericShuffleConsumer(
            coCoAuthorNameShuffleP.getSchema(), coCoAuthorNameShuffleID, allWorkers);
    // schema: (authorId long)

    producers.add(coCoAuthorNameShuffleP);
    producers.add(coAuthorNamesPubsShuffleP);
    producers.add(coAuthorPubsShuffleP);
    producers.add(allPubsShuffleByAuthorP);
    return new StreamingStateWrapper(coCoAuthorNameShuffleC, new DupElim()); // local dupelim
    // schema: (authorId long)
  }

  public static Operator extractName(StreamingStateWrapper erdosN) {
    final SQLiteSetFilter allNames =
        new SQLiteSetFilter(erdosN, "Dictionary", "ID", new String[] {"val"}, singleStringSchema);
    return allNames;
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
      int[] allWorkers, Operator de, ArrayList<Producer> producers) throws Exception {

    final CollectProducer sendToMaster = new CollectProducer(de, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    producers.add(sendToMaster);
    for (int allWorker : allWorkers) {
      result.put(allWorker, producers.toArray(new RootOperator[] {}));
    }
    return result;
  }

  public static SinkRoot getMasterPlan(
      final int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches)
      throws Exception {
    final Consumer serverCollect = new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    return serverPlan;
  }
}
