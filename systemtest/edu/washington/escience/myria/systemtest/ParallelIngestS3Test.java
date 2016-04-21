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
import edu.washington.escience.myria.MyriaConstants;
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
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
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

  Schema lineorderSchema = Schema.ofFields("l_orderkey", Type.LONG_TYPE, "l_linenumber", Type.LONG_TYPE, "l_custkey",
      Type.LONG_TYPE, "l_partkey", Type.LONG_TYPE, "l_suppkey", Type.LONG_TYPE, "l_orderdate", Type.STRING_TYPE,
      "l_orderpriority", Type.STRING_TYPE, "l_shippriority", Type.LONG_TYPE, "l_quantity", Type.FLOAT_TYPE,
      "l_extendedprice", Type.FLOAT_TYPE, "l_ordtotalprice", Type.FLOAT_TYPE, "l_discount", Type.FLOAT_TYPE,
      "l_revenue", Type.LONG_TYPE, "l_supplycost", Type.LONG_TYPE, "l_tax", Type.FLOAT_TYPE, "l_commitdate",
      Type.LONG_TYPE, "l_shipmode", Type.STRING_TYPE);

  String dateTableAddress = "s3a://myria-test/dateOUT.csv";
  String customerTableAddress = "s3a://myria-test/customerOUT.txt";
  String lineorderTableAddress = "s3a://myria-test/lineorderOUT.txt";

  @Test
  public void parallelIngestTest() throws Exception {
    /* Read Source Data from S3 and invoke Server */
    RelationKey relationKey = RelationKey.of("public", "adhoc", "testParallel");

    Map<Integer, RootOperator[]> workerPlansParallelIngest = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      UriSource uriSource = new UriSource(dateTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(uriSource, dateSchema, workerCounterID, workerIDs.length, '|', null, null, 0);
      workerPlansParallelIngest.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKey, true) });
      workerCounterID++;
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansParallelIngest).get();
    assertEquals(2556, server.getDatasetStatus(relationKey).getNumTuples());
  }

  @Test
  public void diffParallelIngestTest() throws Exception {
    /* Read Source Data from S3 and invoke Server */

    /* Ingest in parallel */
    RelationKey relationKeyParallel = RelationKey.of("public", "adhoc", "ingestParallel");
    Map<Integer, RootOperator[]> workerPlansParallelIngest = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      UriSource uriSource = new UriSource(customerTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(uriSource, customerSchema, workerCounterID, workerIDs.length, ',', null, null, 0);
      workerPlansParallelIngest.put(workerID,
          new RootOperator[] { new DbInsert(scanFragment, relationKeyParallel, true) });
      workerCounterID++;
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansParallelIngest).get();
    assertEquals(300000, server.getDatasetStatus(relationKeyParallel).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinator = RelationKey.of("public", "adhoc", "ingestCoordinator");
    server.ingestDataset(relationKeyCoordinator, server.getAliveWorkers(), null, new FileScan(new UriSource(
        customerTableAddress), customerSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(300000, server.getDatasetStatus(relationKeyCoordinator).getNumTuples());

    /* do the diff at the first worker */
    final Map<Integer, RootOperator[]> workerPlansDiff = new HashMap<Integer, RootOperator[]>();

    DbQueryScan scanParallelIngest = new DbQueryScan(relationKeyParallel, customerSchema);
    ExchangePairID receiveParallelIngest = ExchangePairID.newID();
    CollectProducer sendToWorkerParallelIngest =
        new CollectProducer(scanParallelIngest, receiveParallelIngest, workerIDs[0]);

    DbQueryScan scanCoordinatorIngest = new DbQueryScan(relationKeyCoordinator, customerSchema);
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
  public void speedTest() throws URISyntaxException, DbException, InterruptedException, ExecutionException,
      CatalogException, IOException {
    /* Read in parallel and sink */
    Map<Integer, RootOperator[]> workerPlansParallelIngest = new HashMap<Integer, RootOperator[]>();
    int workerCounterID = 1;
    for (int workerID : workerIDs) {
      UriSource uriSource = new UriSource(lineorderTableAddress);
      CSVFileScanFragment scanFragment =
          new CSVFileScanFragment(uriSource, lineorderSchema, workerCounterID, workerIDs.length, '|', null, null, 0);
      workerPlansParallelIngest.put(workerID, new RootOperator[] { new SinkRoot(scanFragment) });
      workerCounterID++;
    }
    final Date startParallelTimer = new Date();
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansParallelIngest).get();
    final double elapsedParallel = (new Date().getTime() - startParallelTimer.getTime()) / 1000.0;

    /* Read through the coordinator and sink */
    FileScan scan = new FileScan(new UriSource(lineorderTableAddress), lineorderSchema, '|', null, null, 0);
    ExchangePairID scatterId = ExchangePairID.newID();
    GenericShuffleProducer masterScatter =
        new GenericShuffleProducer(scan, scatterId, workerIDs, new RoundRobinPartitionFunction(workerIDs.length));
    GenericShuffleConsumer workersGather =
        new GenericShuffleConsumer(lineorderSchema, scatterId, new int[] { MyriaConstants.MASTER_ID });
    Map<Integer, RootOperator[]> workerPlansCoordinatorIngest = new HashMap<Integer, RootOperator[]>();
    for (Integer workerId : workerIDs) {
      workerPlansCoordinatorIngest.put(workerId, new RootOperator[] { new SinkRoot(workersGather) });
    }
    final Date startCoordinator = new Date();
    server.submitQueryPlan(masterScatter, workerPlansCoordinatorIngest).get();
    final double elapsedCoordinator = (new Date().getTime() - startCoordinator.getTime()) / 1000.0;

    LOGGER.warn("TIME SPENT PARALLEL " + elapsedParallel);
    LOGGER.warn("TIME SPENT COORDINATOR " + elapsedCoordinator);

  }
}
