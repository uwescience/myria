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
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.QueryScan;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myriad.parallel.LocalMultiwayProducer;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

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

    final SingleFieldHashPartitionFunction pfOn0 = new SingleFieldHashPartitionFunction(allWorkers.length);
    pfOn0.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final SingleFieldHashPartitionFunction pfOn1 = new SingleFieldHashPartitionFunction(allWorkers.length);
    pfOn1.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 1);

    final QueryScan allPersons = new QueryScan(//
        "SELECT distinct t.subject AS person FROM Triples t " + //
            "JOIN Dictionary dp1 ON t.predicate=dp1.ID " + //
            "JOIN Dictionary d2  ON t.object=d2.ID" + //
            " WHERE dp1.val='rdf:type' AND d2.val='foaf:Person'",//
        Schemas.subjectSchema);
    // schema: (personID long)

    final ShuffleProducer shuffleAllPersonsP = new ShuffleProducer(allPersons, allPersonsShuffleID, allWorkers, pfOn0);
    final ShuffleConsumer shuffleAllPersonsC =
        new ShuffleConsumer(shuffleAllPersonsP.getSchema(), allPersonsShuffleID, allWorkers);
    // schema: (personID long)

    final LocalMultiwayProducer multiPersonProducer =
        new LocalMultiwayProducer(shuffleAllPersonsC, new ExchangePairID[] {
            allPersonsOutLocalMultiWayID, allPersonsInLocalMultiWayID });
    final LocalMultiwayConsumer multiPersonInConsumer =
        new LocalMultiwayConsumer(multiPersonProducer.getSchema(), allPersonsInLocalMultiWayID);
    final LocalMultiwayConsumer multiPersonOutConsumer =
        new LocalMultiwayConsumer(multiPersonProducer.getSchema(), allPersonsOutLocalMultiWayID);

    final ImmutableList<Type> triplesTypes = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE);
    final ImmutableList<String> triplesColumnNames = ImmutableList.of("subject", "predicateName", "object");
    final Schema triplesSchema = new Schema(triplesTypes, triplesColumnNames);

    final QueryScan allTriples =
        new QueryScan("SELECT t.subject,d.val,t.object FROM Triples t,Dictionary d where t.predicate=d.ID",
            triplesSchema);
    // schema: (subject long, predicateName string, object long)

    final LocalMultiwayProducer multiTriplesProducer =
        new LocalMultiwayProducer(allTriples, new ExchangePairID[] {
            allTriplesOutLocalMultiWayID, allTriplesInLocalMultiWayID });
    final LocalMultiwayConsumer multiTriplesInConsumer =
        new LocalMultiwayConsumer(multiTriplesProducer.getSchema(), allTriplesInLocalMultiWayID);
    final LocalMultiwayConsumer multiTriplesOutConsumer =
        new LocalMultiwayConsumer(multiTriplesProducer.getSchema(), allTriplesOutLocalMultiWayID);

    final LocalJoin joinPersonsTriplesIn =
        new LocalJoin(multiPersonInConsumer, multiTriplesInConsumer, new int[] { 0 }, new int[] { 2 });
    // schema: (personID long, subject long, predicateName String, personID long)

    final Project projInPredicates = new Project(new int[] { 2 }, joinPersonsTriplesIn);
    // schema: (predicateName string)

    final LocalJoin joinPersonsTriplesOut =
        new LocalJoin(multiPersonOutConsumer, multiTriplesOutConsumer, new int[] { 0 }, new int[] { 0 });
    // schema: (personID long, personID long, predicateName String, object long)

    final Project projOutPredicates = new Project(new int[] { 2 }, joinPersonsTriplesOut);
    // schema: (predicateName String)

    final Merge union = new Merge(new Operator[] { projInPredicates, projOutPredicates });
    // schema: (predicateName string)

    final DupElim localDE = new DupElim(union); // local dupelim
    // schema: (predicateName string)

    final CollectProducer sendToMaster = new CollectProducer(localDE, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();

    for (int allWorker : allWorkers) {
      result.put(allWorker, new RootOperator[] {
          shuffleAllPersonsP, multiPersonProducer, multiTriplesProducer, sendToMaster });
    }

    return result;
  }

  @Override
  public RootOperator getMasterPlan(int[] allWorkers, final LinkedBlockingQueue<TupleBatch> receivedTupleBatches) {
    final CollectConsumer serverCollect = new CollectConsumer(outputSchema, sendToMasterID, allWorkers);
    TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    return serverPlan;
  }

  public static void main(String[] args) throws Exception {
    System.out.println(new Q9().getWorkerPlan(new int[] { 0, 1, 2, 3, 4 }).get(0)[0]);
  }
}
