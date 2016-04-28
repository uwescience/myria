/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.io.UriSource;
import edu.washington.escience.myria.operator.CSVFileScanFragment;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Difference;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 * 
 */
public class ParallelIngestS3Test extends SystemTestBase {

  Schema dateSchema = Schema.ofFields("d_datekey", Type.LONG_TYPE, "d_date", Type.STRING_TYPE, "d_dayofweek",
      Type.STRING_TYPE, "d_month", Type.STRING_TYPE, "d_year", Type.LONG_TYPE, "d_yearmonthnum", Type.LONG_TYPE,
      "d_yearmonth", Type.STRING_TYPE, "d_daynuminweek", Type.LONG_TYPE, "d_daynuminmonth", Type.LONG_TYPE,
      "d_daynuminyear", Type.LONG_TYPE, "d_monthnuminyear", Type.LONG_TYPE, "d_weeknuminyear", Type.LONG_TYPE,
      "d_sellingseason", Type.STRING_TYPE, "d_lastdayinweekfl", Type.STRING_TYPE, "d_lastdayinmonthfl",
      Type.STRING_TYPE, "d_holidayfl", Type.STRING_TYPE, "d_weekdayfl", Type.STRING_TYPE);

  Schema customerSchema = Schema.ofFields("c_custkey", Type.LONG_TYPE, "c_name", Type.STRING_TYPE, "c_address",
      Type.STRING_TYPE, "c_city", Type.STRING_TYPE, "c_nation_prefix", Type.STRING_TYPE, "c_nation", Type.STRING_TYPE,
      "c_region", Type.STRING_TYPE, "c_phone", Type.STRING_TYPE, "c_mktsegment", Type.STRING_TYPE);

  String dateTableAddress = "s3://myria-test/dateOUT.csv";
  String customerTableAddress = "s3://myria-test/customerOUT.txt";

  @Test
  public void parallelIngestTest() throws Exception {
    RelationKey relationKey = RelationKey.of("public", "adhoc", "testParallel");

    Map<Integer, RootOperator[]> workerPlansParallelIngest = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      AmazonS3Source s3Source = new AmazonS3Source(dateTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(s3Source, dateSchema, workerCounterID, workerIDs.length, '|', null, null, 0);
      workerPlansParallelIngest.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKey, true) });
      workerCounterID++;
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansParallelIngest).get();
    assertEquals(2556, server.getDatasetStatus(relationKey).getNumTuples());
  }

  @Test
  public void diffParallelIngestTest() throws Exception {

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "ingestParallel");
    Map<Integer, RootOperator[]> workerPlansParallelIngest = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      AmazonS3Source s3Source = new AmazonS3Source(customerTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(s3Source, customerSchema, workerCounterID, workerIDs.length, ',', null, null, 0);
      workerPlansParallelIngest.put(workerID, new RootOperator[] { new DbInsert(scanFragment,
          relationKeyParallelIngest, true) });
      workerCounterID++;
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansParallelIngest).get();
    assertEquals(300000, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "ingestCoordinator");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        customerTableAddress), customerSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(300000, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());

    /* do the diff at the first worker */
    final Map<Integer, RootOperator[]> workerPlansDiff = new HashMap<Integer, RootOperator[]>();

    DbQueryScan scanParallelIngest = new DbQueryScan(relationKeyParallelIngest, customerSchema);
    ExchangePairID receiveParallelIngest = ExchangePairID.newID();
    CollectProducer sendToWorkerParallelIngest =
        new CollectProducer(scanParallelIngest, receiveParallelIngest, workerIDs[0]);

    DbQueryScan scanCoordinatorIngest = new DbQueryScan(relationKeyCoordinatorIngest, customerSchema);
    ExchangePairID receiveCoordinatorIngest = ExchangePairID.newID();
    CollectProducer sendToWorkerCoordinatorIngest =
        new CollectProducer(scanCoordinatorIngest, receiveCoordinatorIngest, workerIDs[0]);

    CollectConsumer workerConsumerParallelIngest =
        new CollectConsumer(customerSchema, receiveParallelIngest, workerIDs);
    CollectConsumer workerConsumerCoordinatorIngest =
        new CollectConsumer(customerSchema, receiveCoordinatorIngest, workerIDs);

    RelationKey diffRelationKey = new RelationKey("public", "adhoc", "diffResult");
    Difference diff = new Difference(workerConsumerParallelIngest, workerConsumerCoordinatorIngest);
    DbInsert workerIngest = new DbInsert(diff, diffRelationKey, true);

    workerPlansDiff.put(workerIDs[0], new RootOperator[] {
        sendToWorkerParallelIngest, sendToWorkerCoordinatorIngest, workerIngest });
    workerPlansDiff.put(workerIDs[1], new RootOperator[] { sendToWorkerParallelIngest, sendToWorkerCoordinatorIngest });

    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansDiff).get();

    String data =
        JsonAPIUtils.download("localhost", masterDaemonPort, diffRelationKey.getUserName(), diffRelationKey
            .getProgramName(), diffRelationKey.getRelationName(), "json");

    assertEquals("[]", data);
  }

  @Test
  public void oneTupleTest() throws Exception {
    String oneTupleAddress = "s3://myria-test/sample-parallel-one-tuple.txt";
    Schema oneTupleSchema =
        Schema.ofFields("w", Type.INT_TYPE, "x", Type.INT_TYPE, "y", Type.INT_TYPE, "z", Type.INT_TYPE, "a",
            Type.INT_TYPE);

    RelationKey relationKey = RelationKey.of("public", "adhoc", "testParallelOneTuple");

    Map<Integer, RootOperator[]> workerPlansParallelIngest = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      AmazonS3Source s3Source = new AmazonS3Source(oneTupleAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(s3Source, oneTupleSchema, workerCounterID, workerIDs.length, ',', null, null, 0);
      workerPlansParallelIngest.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKey, true) });
      workerCounterID++;
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansParallelIngest).get();
    assertEquals(1, server.getDatasetStatus(relationKey).getNumTuples());
  }

}
