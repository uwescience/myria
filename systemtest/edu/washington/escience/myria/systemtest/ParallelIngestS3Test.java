/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.coordinator.CatalogException;
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

  String dateTableAddress = "s3://myria-test/dateOUT.csv";

  Schema dateSchema = Schema.ofFields("d_datekey", Type.LONG_TYPE, "d_date", Type.STRING_TYPE, "d_dayofweek",
      Type.STRING_TYPE, "d_month", Type.STRING_TYPE, "d_year", Type.LONG_TYPE, "d_yearmonthnum", Type.LONG_TYPE,
      "d_yearmonth", Type.STRING_TYPE, "d_daynuminweek", Type.LONG_TYPE, "d_daynuminmonth", Type.LONG_TYPE,
      "d_daynuminyear", Type.LONG_TYPE, "d_monthnuminyear", Type.LONG_TYPE, "d_weeknuminyear", Type.LONG_TYPE,
      "d_sellingseason", Type.STRING_TYPE, "d_lastdayinweekfl", Type.STRING_TYPE, "d_lastdayinmonthfl",
      Type.STRING_TYPE, "d_holidayfl", Type.STRING_TYPE, "d_weekdayfl", Type.STRING_TYPE);

  @Test
  public void parallelIngestTest() throws Exception {
    /* Read Source Data from S3 and invoke Server */
    RelationKey relationKey = RelationKey.of("public", "adhoc", "testParallel");

    Map<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      UriSource uriSource = new UriSource(dateTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(uriSource, dateSchema, workerCounterID, workerIDs.length, '|', null, null, 0);
      workerPlans.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKey, true) });
      workerCounterID++;
    }

    SinkRoot masterRoot = new SinkRoot(new EOSSource());
    server.submitQueryPlan(masterRoot, workerPlans).get();
    assertEquals(2556, server.getDatasetStatus(relationKey).getNumTuples());
  }

  @Test
  public void diffParallelIngestTest() throws Exception {
    /* Read Source Data from S3 and invoke Server */

    /* Ingest in parallel */
    RelationKey relationKeyParallel = RelationKey.of("public", "adhoc", "ingestParallel");
    Map<Integer, RootOperator[]> workerPlansParallel = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      UriSource uriSource = new UriSource(dateTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(uriSource, dateSchema, workerCounterID, workerIDs.length, '|', null, null, 0);
      workerPlansParallel.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKeyParallel, true) });
      workerCounterID++;
    }
    SinkRoot masterEmptyRoot = new SinkRoot(new EOSSource());
    server.submitQueryPlan(masterEmptyRoot, workerPlansParallel).get();
    assertEquals(2556, server.getDatasetStatus(relationKeyParallel).getNumTuples());

    String dataOne =
        JsonAPIUtils.download("localhost", masterDaemonPort, relationKeyParallel.getUserName(), relationKeyParallel
            .getProgramName(), relationKeyParallel.getRelationName(), "json");

    LOGGER.warn(dataOne);

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinator = RelationKey.of("public", "adhoc", "ingestCoordinator");
    server.ingestDataset(relationKeyCoordinator, server.getAliveWorkers(), null, new FileScan(new UriSource(
        dateTableAddress), dateSchema, '|', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(2556, server.getDatasetStatus(relationKeyCoordinator).getNumTuples());

    /* do the diff at the first worker */
    final Map<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();

    DbQueryScan scanParallelIngest = new DbQueryScan(relationKeyParallel, dateSchema);
    ExchangePairID masterReceiveParallelIngest = ExchangePairID.newID();
    CollectProducer sendToMasterParallelIngest =
        new CollectProducer(scanParallelIngest, masterReceiveParallelIngest, workerIDs[0]);

    DbQueryScan scanCoordinatorIngest = new DbQueryScan(relationKeyCoordinator, dateSchema);
    ExchangePairID masterReceiveCoordinatorIngest = ExchangePairID.newID();
    CollectProducer workerProduceCoordinatorIngest =
        new CollectProducer(scanCoordinatorIngest, masterReceiveCoordinatorIngest, workerIDs[0]);

    CollectConsumer workerConsumerParallelIngest =
        new CollectConsumer(dateSchema, masterReceiveParallelIngest, workerIDs);
    CollectConsumer masterConsumerCoordinatorIngest =
        new CollectConsumer(dateSchema, masterReceiveCoordinatorIngest, workerIDs);
    RelationKey diffRelationKey = new RelationKey("public", "adhoc", "diffResult");
    Difference diff = new Difference(workerConsumerParallelIngest, masterConsumerCoordinatorIngest);
    DbInsert workerRoot = new DbInsert(diff, diffRelationKey, true);

    workerPlans.put(workerIDs[0], new RootOperator[] {
        sendToMasterParallelIngest, workerProduceCoordinatorIngest, workerRoot });
    workerPlans.put(workerIDs[1], new RootOperator[] { sendToMasterParallelIngest, workerProduceCoordinatorIngest });

    server.submitQueryPlan(masterEmptyRoot, workerPlans).get();

    String data =
        JsonAPIUtils.download("localhost", masterDaemonPort, diffRelationKey.getUserName(), diffRelationKey
            .getProgramName(), diffRelationKey.getRelationName(), "json");

    LOGGER.warn(data);

    assertEquals("[]", data);
  }

  @Test
  public void speedTest() throws URISyntaxException, DbException, InterruptedException, ExecutionException,
      CatalogException, IOException {

    Schema customerSchema =
        Schema.ofFields("c_custkey", Type.LONG_TYPE, "c_name", Type.STRING_TYPE, "c_address", Type.STRING_TYPE,
            "c_city", Type.STRING_TYPE, "c_nation_prefix", Type.STRING_TYPE, "c_nation", Type.STRING_TYPE, "c_region",
            Type.STRING_TYPE, "c_phone", Type.STRING_TYPE, "c_mktsegment", Type.STRING_TYPE);

    String customerTableAddress = "s3://myria-test/customerOUT.txt";

    /* Ingest in parallel */
    RelationKey relationKeyParallel = RelationKey.of("public", "adhoc", "ingestParallel");
    Map<Integer, RootOperator[]> workerPlansParallel = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      UriSource uriSource = new UriSource(customerTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(uriSource, customerSchema, workerCounterID, workerIDs.length, ',', null, null, 0);
      workerPlansParallel.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKeyParallel, true) });
      workerCounterID++;
    }
    SinkRoot masterEmptyRoot = new SinkRoot(new EOSSource());
    final Date startParallel = new Date();
    server.submitQueryPlan(masterEmptyRoot, workerPlansParallel).get();
    final double elapsedParallel = (new Date().getTime() - startParallel.getTime()) / 1000.0;

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinator = RelationKey.of("public", "adhoc", "ingestCoordinator");
    final Date startCoordinator = new Date();
    server.ingestDataset(relationKeyCoordinator, server.getAliveWorkers(), null, new FileScan(new UriSource(
        customerTableAddress), customerSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    final double elapsedCoordinator = (new Date().getTime() - startCoordinator.getTime()) / 1000.0;

    LOGGER.warn("TIME SPENT " + elapsedParallel);
    LOGGER.warn("TIME SPENT " + elapsedCoordinator);

  }
}
