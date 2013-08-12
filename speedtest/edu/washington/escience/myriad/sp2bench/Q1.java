package edu.washington.escience.myriad.sp2bench;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.DbQueryScan;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.operator.TBQueueExporter;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.ExchangePairID;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

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

    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(allWorkers.length);
    pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, 0);

    final ShuffleProducer shuffleJournalsP = new ShuffleProducer(allJournals, allJournalsShuffleID, allWorkers, pf);
    final ShuffleConsumer shuffleJournalsC =
        new ShuffleConsumer(shuffleJournalsP.getSchema(), allJournalsShuffleID, allWorkers);

    final ShuffleProducer shuffleWithTitleP =
        new ShuffleProducer(allWithTheTitle, allWithTitleShuffleID, allWorkers, pf);
    final ShuffleConsumer shuffleWithTitleC =
        new ShuffleConsumer(shuffleWithTitleP.getSchema(), allWithTitleShuffleID, allWorkers);

    final ShuffleProducer shuffleIssuedYearP =
        new ShuffleProducer(allIssuedYear, allIssuedYearShuffleID, allWorkers, pf);
    final ShuffleConsumer shuffleIssuedYearC =
        new ShuffleConsumer(shuffleIssuedYearP.getSchema(), allIssuedYearShuffleID, allWorkers);

    final LocalJoin joinJournalTitle =
        new LocalJoin(shuffleJournalsC, shuffleWithTitleC, new int[] { 0 }, new int[] { 0 });

    final LocalJoin joinJournalTitleYear =
        new LocalJoin(joinJournalTitle, shuffleIssuedYearC, new int[] { 0 }, new int[] { 0 });

    final Project finalProject = new Project(new int[] { 3 }, joinJournalTitleYear);

    final CollectProducer sendToMaster = new CollectProducer(finalProject, sendToMasterID, 0);

    final Map<Integer, RootOperator[]> result = new HashMap<Integer, RootOperator[]>();
    for (int worker : allWorkers) {
      result.put(worker, new RootOperator[] { sendToMaster, shuffleIssuedYearP, shuffleWithTitleP, shuffleJournalsP });
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
}
