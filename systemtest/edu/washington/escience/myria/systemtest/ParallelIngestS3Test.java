/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
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

  String dateTableAddress = "s3://myria-test/dateOUT.csv";

  @Test
  public void parallelIngestTest() throws Exception {
    RelationKey dateRelationKey = RelationKey.of("public", "adhoc", "testParallel");
    server.parallelIngestDataset(dateRelationKey, dateSchema, '|', null, null, 0, dateTableAddress, null);
    assertEquals(2556, server.getDatasetStatus(dateRelationKey).getNumTuples());
  }

  public void diffHelperMethod(final RelationKey relationKeyParallelIngest,
      final RelationKey relationKeyCoordinatorIngest, final Schema schema) throws Exception {

    /* WholeTupleHashPartition the tuples from the coordinator ingest */
    DbQueryScan scanCoordinatorIngest = new DbQueryScan(relationKeyCoordinatorIngest, schema);
    ExchangePairID receiveCoordinatorIngest = ExchangePairID.newID();
    GenericShuffleProducer sendToWorkerCoordinatorIngest =
        new GenericShuffleProducer(scanCoordinatorIngest, receiveCoordinatorIngest, new int[] {
            workerIDs[0], workerIDs[1] }, new WholeTupleHashPartitionFunction(workerIDs.length));
    GenericShuffleConsumer workerConsumerCoordinatorIngest =
        new GenericShuffleConsumer(schema, receiveCoordinatorIngest, new int[] { workerIDs[0], workerIDs[1] });
    DbInsert workerCoordinatorIngest =
        new DbInsert(workerConsumerCoordinatorIngest, relationKeyCoordinatorIngest, true);
    Map<Integer, RootOperator[]> workerPlansHashCoordinatorIngest = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlansHashCoordinatorIngest.put(workerID, new RootOperator[] {
          sendToWorkerCoordinatorIngest, workerCoordinatorIngest });
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansHashCoordinatorIngest).get();

    /* WholeTupleHashPartition the tuples from the parallel ingest */
    DbQueryScan scanParallelIngest = new DbQueryScan(relationKeyParallelIngest, schema);
    ExchangePairID receiveParallelIngest = ExchangePairID.newID();
    GenericShuffleProducer sendToWorkerParallelIngest =
        new GenericShuffleProducer(scanParallelIngest, receiveParallelIngest, new int[] { workerIDs[0], workerIDs[1] },
            new WholeTupleHashPartitionFunction(workerIDs.length));
    GenericShuffleConsumer workerConsumerParallelIngest =
        new GenericShuffleConsumer(schema, receiveParallelIngest, new int[] { workerIDs[0], workerIDs[1] });
    DbInsert workerIngest = new DbInsert(workerConsumerParallelIngest, relationKeyParallelIngest, true);
    Map<Integer, RootOperator[]> workerPlansHashParallelIngest = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlansHashParallelIngest.put(workerID, new RootOperator[] { sendToWorkerParallelIngest, workerIngest });
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansHashParallelIngest).get();

    /* Run the diff at each worker */
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
  public void parallelIngestSimpleDiff() throws Exception {
    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "ingestParallel");
    server.parallelIngestDataset(relationKeyParallelIngest, dateSchema, '|', null, null, 0, dateTableAddress, server
        .getAliveWorkers());
    assertEquals(2556, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator and WholeTupleHashPartition the result */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "ingestCoordinator");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        dateTableAddress), dateSchema, '|', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(2556, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());

    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, dateSchema);
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

  @Test
  /**
   * With two workers, this covers the case where Worker #2 has a long string on the first row -- which should be discarded by Worker#2
   **/
  public void truncatedBeginningFragmentTest() throws Exception {
    String beginningTrailAddress = "s3://myria-test/TestLongBeginningTrail.txt";
    Schema beginningTrailSchema =
        Schema.ofFields("w", Type.STRING_TYPE, "x", Type.INT_TYPE, "y", Type.INT_TYPE, "z", Type.INT_TYPE, "a",
            Type.INT_TYPE);

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "beginningParallel");
    server.parallelIngestDataset(relationKeyParallelIngest, beginningTrailSchema, ',', null, null, 0,
        beginningTrailAddress, server.getAliveWorkers());
    assertEquals(4, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "beginningCoordinator");
    server.ingestDataset(relationKeyCoordinatorIngest, new HashSet<Integer>(Arrays.asList(workerIDs[0], workerIDs[1])),
        null, new FileScan(new UriSource(beginningTrailAddress), beginningTrailSchema, ',', null, null, 0),
        new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(4, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());
    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, beginningTrailSchema);
  }

  @Test
  /**
   * With two workers, this covers the case where Worker #1 has a long string on the last row
   **/
  public void truncatedEndFragmentTestLF() throws Exception {
    String endTrailAddress = "s3://myria-test/TestLongEndTrail_nlines.txt";
    Schema endTrailSchema =
        Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE, "z", Type.INT_TYPE, "a", Type.INT_TYPE, "w",
            Type.STRING_TYPE);

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "endParallel_nlines");
    server.parallelIngestDataset(relationKeyParallelIngest, endTrailSchema, ',', null, null, 0, endTrailAddress, server
        .getAliveWorkers());
    assertEquals(4, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "endCoordinator_nlines");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        endTrailAddress), endTrailSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(4, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());
    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, endTrailSchema);
  }

  @Test
  /**
   * Test for truncated end for carriage return
   **/
  public void truncatedEndFragmentCR() throws Exception {
    String fileAddress = "s3://myria-test/TestLongEndTrail_rlines.txt";
    Schema fileSchema =
        Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE, "z", Type.INT_TYPE, "a", Type.INT_TYPE, "w",
            Type.STRING_TYPE);

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "endParallel_rlines");
    server.parallelIngestDataset(relationKeyParallelIngest, fileSchema, ',', null, null, 0, fileAddress, server
        .getAliveWorkers());
    assertEquals(4, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "endCoordinator_rlines");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        fileAddress), fileSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(4, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());
    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, fileSchema);

  }

  @Test
  /**
   * Test for truncated end for carriage return and line feed
   **/
  public void truncatedEndFragmentCRLF() throws Exception {
    String fileAddress = "s3://myria-test/TestLongEndTrail_rnlines.txt";
    Schema fileSchema =
        Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE, "z", Type.INT_TYPE, "a", Type.INT_TYPE, "w",
            Type.STRING_TYPE);

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "endParallel_rnlines");
    server.parallelIngestDataset(relationKeyParallelIngest, fileSchema, ',', null, null, 0, fileAddress, server
        .getAliveWorkers());
    assertEquals(4, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "endCoordinator_rnlines");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        fileAddress), fileSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(4, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());
    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, fileSchema);
  }

  @Test
  /**
   * Testing a perfect split case
   **/
  public void perfectRowSplitLF() throws Exception {
    String perfectSplitAddress = "s3://myria-test/PerfectSplit_nlines.txt";
    Schema perfectSplitSchema = Schema.ofFields("x", Type.INT_TYPE, "w", Type.STRING_TYPE);

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "perfectParallel_nlines");
    server.parallelIngestDataset(relationKeyParallelIngest, perfectSplitSchema, ',', null, null, 0,
        perfectSplitAddress, server.getAliveWorkers());
    assertEquals(4, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "perfectCoordinator_nlines");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        perfectSplitAddress), perfectSplitSchema, ',', null, null, 0),
        new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(4, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());
    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, perfectSplitSchema);
  }

  @Test
  public void perfectRowSplitCR() throws Exception {
    String fileAddress = "s3://myria-test/PerfectSplit_rlines.txt";
    Schema fileSchema = Schema.ofFields("x", Type.INT_TYPE, "w", Type.STRING_TYPE);

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "perfectParallel_rlines");
    server.parallelIngestDataset(relationKeyParallelIngest, fileSchema, ',', null, null, 0, fileAddress, server
        .getAliveWorkers());
    assertEquals(4, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "perfectCoordinator_rlines");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        fileAddress), fileSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(4, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());
    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, fileSchema);
  }

  @Test
  public void perfectRowSplitCRLF() throws Exception {
    String fileAddress = "s3://myria-test/PerfectSplit_rnlines.txt";
    Schema fileSchema = Schema.ofFields("x", Type.INT_TYPE, "w", Type.STRING_TYPE);

    /* Ingest in parallel */
    RelationKey relationKeyParallelIngest = RelationKey.of("public", "adhoc", "perfectParallel_rnlines");
    server.parallelIngestDataset(relationKeyParallelIngest, fileSchema, ',', null, null, 0, fileAddress, server
        .getAliveWorkers());
    assertEquals(4, server.getDatasetStatus(relationKeyParallelIngest).getNumTuples());

    /* Ingest the through the coordinator */
    RelationKey relationKeyCoordinatorIngest = RelationKey.of("public", "adhoc", "perfectCoordinator_rnlines");
    server.ingestDataset(relationKeyCoordinatorIngest, server.getAliveWorkers(), null, new FileScan(new UriSource(
        fileAddress), fileSchema, ',', null, null, 0), new RoundRobinPartitionFunction(workerIDs.length));
    assertEquals(4, server.getDatasetStatus(relationKeyCoordinatorIngest).getNumTuples());
    diffHelperMethod(relationKeyParallelIngest, relationKeyCoordinatorIngest, fileSchema);
  }

}
