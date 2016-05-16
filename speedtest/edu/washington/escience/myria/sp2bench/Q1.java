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
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

public class Q1 implements QueryPlanGenerator {

  final static ImmutableList<Type> outputTypes = ImmutableList.of(Type.STRING_TYPE);
  final static ImmutableList<String> outputColumnNames = ImmutableList.of("yr");
  final static Schema outputSchema = new Schema(outputTypes, outputColumnNames);

  final static ImmutableList<Type> subjectTypes = ImmutableList.of(Type.LONG_TYPE);
  final static ImmutableList<String> subjectColumnNames = ImmutableList.of("subject");

  final static Schema subjectSchema = new Schema(subjectTypes, subjectColumnNames);
  final ExchangePairID sendToMasterID = ExchangePairID.newID();

  @Override
  public Map<Integer, RootOperator[]> getWorkerPlan(int[] allWorkers) throws Exception {
    final ExchangePairID allJournalsShuffleID = ExchangePairID.newID();
    final ExchangePairID allWithTitleShuffleID = ExchangePairID.newID();
    final ExchangePairID allIssuedYearShuffleID = ExchangePairID.newID();

    final ImmutableList<Type> subjectYearTypes = ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> subjectYearColumnNames = ImmutableList.of("subject", "year");

    final Schema subjectYearSchema = new Schema(subjectYearTypes, subjectYearColumnNames);

    final DbQueryScan allJournals =
        new DbQueryScan(
            "select t.subject from Triples t,Dictionary dtype, Dictionary djournal where t.predicate=dtype.ID and dtype.val='rdf:type' and t.object=djournal.ID and djournal.val='bench:Journal'",
            subjectSchema);
    final DbQueryScan allWithTheTitle =
        new DbQueryScan(
            "select t.subject from Triples t,Dictionary dtype, Dictionary dtitle  where t.predicate=dtype.ID and dtype.val='dc:title' and t.object = dtitle.ID and dtitle.val='\"Journal 1 (1940)\"^^xsd:string';",
            subjectSchema);

    final DbQueryScan allIssuedYear =
        new DbQueryScan(
            "select t.subject,dyear.val from Triples t, Dictionary dtype, Dictionary dyear where t.predicate=dtype.ID and dtype.val='dcterms:issued' and t.object=dyear.ID;",
            subjectYearSchema);

    final SingleFieldHashPartitionFunction pf =
        new SingleFieldHashPartitionFunction(allWorkers.length, 0);

    final GenericShuffleProducer shuffleJournalsP =
        new GenericShuffleProducer(allJournals, allJournalsShuffleID, allWorkers, pf);
    final GenericShuffleConsumer shuffleJournalsC =
        new GenericShuffleConsumer(shuffleJournalsP.getSchema(), allJournalsShuffleID, allWorkers);

    final GenericShuffleProducer shuffleWithTitleP =
        new GenericShuffleProducer(allWithTheTitle, allWithTitleShuffleID, allWorkers, pf);
    final GenericShuffleConsumer shuffleWithTitleC =
        new GenericShuffleConsumer(
            shuffleWithTitleP.getSchema(), allWithTitleShuffleID, allWorkers);

    final GenericShuffleProducer shuffleIssuedYearP =
        new GenericShuffleProducer(allIssuedYear, allIssuedYearShuffleID, allWorkers, pf);
    final GenericShuffleConsumer shuffleIssuedYearC =
        new GenericShuffleConsumer(
            shuffleIssuedYearP.getSchema(), allIssuedYearShuffleID, allWorkers);

    final SymmetricHashJoin joinJournalTitle =
        new SymmetricHashJoin(shuffleJournalsC, shuffleWithTitleC, new int[] {0}, new int[] {0});

    final SymmetricHashJoin joinJournalTitleYear =
        new SymmetricHashJoin(joinJournalTitle, shuffleIssuedYearC, new int[] {0}, new int[] {0});

    final Apply finalColSelect = Applys.columnSelect(joinJournalTitleYear, 3);

    final CollectProducer sendToMaster = new CollectProducer(finalColSelect, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    for (int worker : allWorkers) {
      result.put(
          worker,
          new RootOperator[] {
            sendToMaster, shuffleIssuedYearP, shuffleWithTitleP, shuffleJournalsP
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
}
