package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.EmptySink;
import edu.washington.escience.myria.operator.InMemoryOrderBy;
import edu.washington.escience.myria.operator.LeapFrogJoin;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.JsonAPIUtils;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class MultiwayJoinTest extends SystemTestBase {

  @Test
  public void twoWayJoinUsingMultiwayJoinOperator() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    /* Step 1: generate test data */
    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    server.addDatasetToCatalog(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
        workerIDs[1])));
    server.addDatasetToCatalog(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
        workerIDs[1])));

    /* Step 2: submit JSON query plan */
    File queryJson = new File("./jsonQueries/multiwayJoin_shumo/twoWayJoinSystemTest.json");
    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(3)) {
      Thread.sleep(100);
    }

    /* Step 3: collect data from 2 workers */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ImmutableList<Type> outputTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> outputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);
    final RelationKey JOIN_TEST_RESULT = RelationKey.of("test", "test", "two_way_join_test");
    final DbQueryScan scan = new DbQueryScan(JOIN_TEST_RESULT, outputSchema);
    final CollectProducer cp1 = new CollectProducer(scan, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp1 });
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    EmptySink serverPlan = new EmptySink(queueStore);
    server.submitQueryPlan(serverPlan, workerPlans).get();

    /* Step 4: verify the result. */
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    TupleBatch tb = null;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    TestUtils.assertTupleBagEqual(expectedResult, resultBag);
  }

  @Test
  public void twoWayJoinUsingMultiwayJoinOperator2() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    /* Step 1: ingetst data */
    /* Table 1 */
    createTable(workerIDs[0], JOIN_TEST_TABLE_1, "id long, name varchar(20)");
    createTable(workerIDs[1], JOIN_TEST_TABLE_1, "id long, name varchar(20)");

    /* Table 2 */
    createTable(workerIDs[0], JOIN_TEST_TABLE_2, "id long, name varchar(20)");
    createTable(workerIDs[1], JOIN_TEST_TABLE_2, "id long, name varchar(20)");

    final TupleBatchBuffer tb1w1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    final TupleBatchBuffer tb2w1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    final TupleBatchBuffer tb1w2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    final TupleBatchBuffer tb2w2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);

    Path table1Worker1FilePath = Paths.get("testdata", "multiwayjoin", "testtable1_worker1.csv");
    Path table2Worker1FilePath = Paths.get("testdata", "multiwayjoin", "testtable2_worker1.csv");
    Path table1Worker2FilePath = Paths.get("testdata", "multiwayjoin", "testtable1_worker2.csv");
    Path table2Worker2FilePath = Paths.get("testdata", "multiwayjoin", "testtable2_worker2.csv");

    CSVParser csv1 = new CSVParser(new FileReader(table1Worker1FilePath.toFile()), CSVFormat.DEFAULT);
    CSVParser csv2 = new CSVParser(new FileReader(table2Worker1FilePath.toFile()), CSVFormat.DEFAULT);
    CSVParser csv3 = new CSVParser(new FileReader(table1Worker2FilePath.toFile()), CSVFormat.DEFAULT);
    CSVParser csv4 = new CSVParser(new FileReader(table2Worker2FilePath.toFile()), CSVFormat.DEFAULT);

    for (final CSVRecord record : csv1) {
      tb1w1.putLong(0, Long.parseLong(record.get(0)));
      tb1w1.putString(1, record.get(1));
    }
    csv1.close();

    for (final CSVRecord record : csv2) {
      tb2w1.putLong(0, Long.parseLong(record.get(0)));
      tb2w1.putString(1, record.get(1));
    }
    csv2.close();

    for (final CSVRecord record : csv3) {
      tb1w2.putLong(0, Long.parseLong(record.get(0)));
      tb1w2.putString(1, record.get(1));
    }
    csv3.close();

    for (final CSVRecord record : csv4) {
      tb2w2.putLong(0, Long.parseLong(record.get(0)));
      tb2w2.putString(1, record.get(1));
    }
    csv4.close();

    final TupleBatchBuffer table1 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table1.unionAll(tb1w1);
    table1.unionAll(tb1w2);

    final TupleBatchBuffer table2 = new TupleBatchBuffer(JOIN_INPUT_SCHEMA);
    table2.unionAll(tb2w1);
    table2.unionAll(tb2w2);

    final HashMap<Tuple, Integer> expectedResult = TestUtils.naturalJoin(table1, table2, 0, 0);

    TupleBatch tb = null;
    while ((tb = tb1w1.popAny()) != null) {
      insert(workerIDs[0], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tb2w1.popAny()) != null) {
      insert(workerIDs[0], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tb1w2.popAny()) != null) {
      insert(workerIDs[1], JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, tb);
    }
    while ((tb = tb2w2.popAny()) != null) {
      insert(workerIDs[1], JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, tb);
    }

    /* import dataset to catalog */
    server.addDatasetToCatalog(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
        workerIDs[1])));
    server.addDatasetToCatalog(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
        workerIDs[1])));

    /* Step 2: do the query */
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final ExchangePairID table1ShuffleID = ExchangePairID.newID();
    final ExchangePairID table2ShuffleID = ExchangePairID.newID();
    final SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(2, 0);

    final ImmutableList<Type> outputTypes =
        ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE, Type.LONG_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> outputColumnNames = ImmutableList.of("id1", "name1", "id2", "name2");
    final Schema outputSchema = new Schema(outputTypes, outputColumnNames);

    final DbQueryScan scan1 = new DbQueryScan(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA);

    final GenericShuffleProducer sp1 =
        new GenericShuffleProducer(scan1, table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc1 =
        new GenericShuffleConsumer(sp1.getSchema(), table1ShuffleID, new int[] { workerIDs[0], workerIDs[1] });
    final GenericShuffleProducer sp2 =
        new GenericShuffleProducer(scan2, table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] }, pf);
    final GenericShuffleConsumer sc2 =
        new GenericShuffleConsumer(sp2.getSchema(), table2ShuffleID, new int[] { workerIDs[0], workerIDs[1] });

    final InMemoryOrderBy o1 = new InMemoryOrderBy(sc1, new int[] { 0, 1 }, new boolean[] { true, true });
    final InMemoryOrderBy o2 = new InMemoryOrderBy(sc2, new int[] { 0, 1 }, new boolean[] { true, true });

    int[][][] fieldMap = new int[][][] { { { 0, 0 }, { 1, 0 } } };
    int[][] outputMap = new int[][] { { 0, 0 }, { 0, 1 }, { 1, 0 }, { 1, 1 } };

    final LeapFrogJoin localjoin =
        new LeapFrogJoin(new Operator[] { o1, o2 }, fieldMap, outputMap, outputColumnNames, new boolean[] {
            false, false });
    localjoin.getSchema();
    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { sp1, sp2, cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { sp1, sp2, cp1 });

    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    EmptySink serverPlan = new EmptySink(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).get();
    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    tb = null;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }

    final HashMap<Tuple, Integer> resultBag = TestUtils.tupleBatchToTupleBag(actualResult);
    TestUtils.assertTupleBagEqual(expectedResult, resultBag);
  }
}
