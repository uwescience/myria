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
import edu.washington.escience.myria.io.UriSource;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.Difference;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.WholeTupleHashPartitionFunction;
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
    RelationKey dateRelationKey = RelationKey.of("public", "adhoc", "testParallel");
    server.parallelIngestDataset(dateRelationKey, dateSchema, '|', null, null, 0, dateTableAddress, null);
    assertEquals(2556, server.getDatasetStatus(dateRelationKey).getNumTuples());
  }

  @Test
  public void diffParallelIngestTest() throws Exception {

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "ingestParallel");
    server.parallelIngestDataset(relationKeyParallelIngest, customerSchema, ',', null, null, 0, customerTableAddress,
        server.getAliveWorkers());
    assertEquals(300000, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* WholeTupleHashPartition the tuples from the parallel ingest */
    DbQueryScan scanIngest = new DbQueryScan(relationKeyParallelIngest, customerSchema);
    ExchangePairID receiveParallelIngest = ExchangePairID.newID();
    GenericShuffleProducer sendToWorkerParallelIngest =
        new GenericShuffleProducer(scanIngest, receiveParallelIngest, workerIDs, new WholeTupleHashPartitionFunction(
            workerIDs.length));
    GenericShuffleConsumer workerConsumerParallelIngest =
        new GenericShuffleConsumer(customerSchema, receiveParallelIngest, workerIDs);
    DbInsert workerIngest = new DbInsert(workerConsumerParallelIngest, relationKeyParallelIngest, true);
    Map<Integer, RootOperator[]> workerPlansHashParallelIngest = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlansHashParallelIngest.put(workerID, new RootOperator[] { sendToWorkerParallelIngest, workerIngest });
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansHashParallelIngest).get();

    /* Ingest the through the coordinator and WholeTupleHashPartition the result */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "ingestCoordinator");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        customerTableAddress), customerSchema, ',', null, null, 0), new WholeTupleHashPartitionFunction(
        workerIDs.length));
    assertEquals(300000, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());

    /* Run the diff at each worker */
    DbQueryScan scanParallelIngest = new DbQueryScan(relationKeyParallelIngest, customerSchema);
    DbQueryScan scanCoordinatorIngest = new DbQueryScan(relationKeyCoordinatorIngest, customerSchema);
    RelationKey diffRelationKey = new RelationKey("public", "adhoc", "diffResult");
    Difference diff = new Difference(scanParallelIngest, scanCoordinatorIngest);
    DbInsert diffResult = new DbInsert(diff, diffRelationKey, true);
    final Map<Integer, RootOperator[]> workerPlansDiff = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlansDiff.put(workerID, new RootOperator[] { diffResult });
    }
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
    server.parallelIngestDataset(relationKey, oneTupleSchema, ',', null, null, 0, oneTupleAddress, null);
    assertEquals(1, server.getDatasetStatus(relationKey).getNumTuples());
  }
}
