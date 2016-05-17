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
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.StreamingStateWrapper;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.UnionAll;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.LocalMultiwayConsumer;
import edu.washington.escience.myria.operator.network.LocalMultiwayProducer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class Q9 implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("person_predicates");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {

    final ExchangePairID allPersonsShuffleID = ExchangePairID.newID();
    final ExchangePairID allPersonsOutLocalMultiWayID = ExchangePairID.newID();
    final ExchangePairID allPersonsInLocalMultiWayID = ExchangePairID.newID();
    final ExchangePairID allTriplesOutLocalMultiWayID = ExchangePairID.newID();
    final ExchangePairID allTriplesInLocalMultiWayID = ExchangePairID.newID();

    final SingleFieldHashPartitionFunction pfOn0 =
        new SingleFieldHashPartitionFunction(allWorkers.length, 0);

    final DbQueryScan allPersons =
        new DbQueryScan( //
            "SELECT distinct t.subject AS person FROM Triples t "
                + //
                "JOIN Dictionary dp1 ON t.predicate=dp1.ID "
                + //
                "JOIN Dictionary d2  ON t.object=d2.ID"
                + //
                " WHERE dp1.val='rdf:type' AND d2.val='foaf:Person'", //
            Schemas.subjectSchema);
    // schema: (personID long)

    final GenericShuffleProducer shuffleAllPersonsP =
        new GenericShuffleProducer(allPersons, allPersonsShuffleID, allWorkers, pfOn0);
    final GenericShuffleConsumer shuffleAllPersonsC =
        new GenericShuffleConsumer(shuffleAllPersonsP.getSchema(), allPersonsShuffleID, allWorkers);
    // schema: (personID long)

    final LocalMultiwayProducer multiPersonProducer =
        new LocalMultiwayProducer(
            shuffleAllPersonsC,
            new ExchangePairID[] {allPersonsOutLocalMultiWayID, allPersonsInLocalMultiWayID});
    final LocalMultiwayConsumer multiPersonInConsumer =
        new LocalMultiwayConsumer(multiPersonProducer.getSchema(), allPersonsInLocalMultiWayID);
    final LocalMultiwayConsumer multiPersonOutConsumer =
        new LocalMultiwayConsumer(multiPersonProducer.getSchema(), allPersonsOutLocalMultiWayID);

    final ImmutableList<Type> triplesTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> triplesColumnNames =
        ImmutableList.of("subject", "predicateName", "object");
    final Schema triplesSchema = new Schema(triplesTypes, triplesColumnNames);

    final DbQueryScan allTriples =
        new DbQueryScan(
            "SELECT t.subject,d.val,t.object FROM Triples t,Dictionary d where t.predicate=d.ID",
            triplesSchema);
    // schema: (subject long, predicateName string, object long)

    final LocalMultiwayProducer multiTriplesProducer =
        new LocalMultiwayProducer(
            allTriples,
            new ExchangePairID[] {allTriplesOutLocalMultiWayID, allTriplesInLocalMultiWayID});
    final LocalMultiwayConsumer multiTriplesInConsumer =
        new LocalMultiwayConsumer(multiTriplesProducer.getSchema(), allTriplesInLocalMultiWayID);
    final LocalMultiwayConsumer multiTriplesOutConsumer =
        new LocalMultiwayConsumer(multiTriplesProducer.getSchema(), allTriplesOutLocalMultiWayID);

    final SymmetricHashJoin joinPersonsTriplesIn =
        new SymmetricHashJoin(
            multiPersonInConsumer, multiTriplesInConsumer, new int[] {0}, new int[] {2});
    // schema: (personID long, subject long, predicateName String, personID long)

    final Apply projInPredicates = Applys.columnSelect(joinPersonsTriplesIn, 2);
    // schema: (predicateName string)

    final SymmetricHashJoin joinPersonsTriplesOut =
        new SymmetricHashJoin(
            multiPersonOutConsumer, multiTriplesOutConsumer, new int[] {0}, new int[] {0});
    // schema: (personID long, personID long, predicateName String, object long)

    final Apply projOutPredicates = Applys.columnSelect(joinPersonsTriplesOut, 2);
    // schema: (predicateName String)

    final UnionAll union = new UnionAll(new Operator[] {projInPredicates, projOutPredicates});
    // schema: (predicateName string)

    final StreamingStateWrapper localDE = new StreamingStateWrapper(union, new DupElim());
    // schema: (predicateName string)

    final CollectProducer sendToMaster = new CollectProducer(localDE, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();

    for (int allWorker : allWorkers) {
      result.put(
          allWorker,
          new RootOperator[] {
            shuffleAllPersonsP, multiPersonProducer, multiTriplesProducer, sendToMaster
          });
    }

    return result;
  }

  @Override
  public RootOperator getMasterPlan(
      int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    return serverPlan;
  }

  public static void main(String[] args) throws Exception {
    System.out.println(new Q9().getWorkerPlan(new int[] {0, 1, 2, 3, 4}).get(0)[0]);
  }
}
