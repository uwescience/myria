package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.InMemoryOrderBy;
import edu.washington.escience.myria.operator.LeapFrogJoin;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.GenericShuffleConsumer;
import edu.washington.escience.myria.parallel.GenericShuffleProducer;
import edu.washington.escience.myria.parallel.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;
import edu.washington.escience.myria.util.JsonAPIUtils;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class MultiwayJoinTest extends SystemTestBase {

  public static final Schema TWITTER_R_SCHEMA = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE),
      ImmutableList.of("r_x", "r_y"));
  public static final Schema TWITTER_S_SCHEMA = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE),
      ImmutableList.of("s_y", "s_z"));
  public static final Schema TWITTER_T_SCHEMA = new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE),
      ImmutableList.of("t_x", "t_z"));
  public static final RelationKey TWITTER_R = RelationKey.of("test", "triangleJoin", "twitterR");
  public static final RelationKey TWITTER_S = RelationKey.of("test", "triangleJoin", "twitterS");
  public static final RelationKey TWITTER_T = RelationKey.of("test", "triangleJoin", "twitterT");

  /**
   * Insert twitterK into database
   * 
   * @throws CatalogException catalog exception
   * @throws IOException I/O exception
   * @throws DbException DB exception
   * @throws InterruptedException
   */
  private void insertTwitterTables() throws CatalogException, IOException, DbException, InterruptedException {
    /* Table R */
    createTable(workerIDs[0], TWITTER_R, "r_x long, r_y long");

    /* Table S */
    createTable(workerIDs[0], TWITTER_S, "s_y long, s_z long");

    /* Table T */
    createTable(workerIDs[0], TWITTER_T, "t_x long, t_z long");

    final TupleBatchBuffer tbr = new TupleBatchBuffer(TWITTER_R_SCHEMA);
    final TupleBatchBuffer tbs = new TupleBatchBuffer(TWITTER_S_SCHEMA);
    final TupleBatchBuffer tbt = new TupleBatchBuffer(TWITTER_T_SCHEMA);

    Path twitterFilePath = Paths.get("testdata", "twitter", "TwitterK.csv");

    CSVReader csv = new CSVReader(new FileReader(twitterFilePath.toFile()));

    String[] line;

    while ((line = csv.readNext()) != null) {
      long follower = Long.parseLong(line[0]);
      long followee = Long.parseLong(line[1]);
      tbr.putLong(0, follower);
      tbr.putLong(1, followee);
      tbs.putLong(0, follower);
      tbs.putLong(1, followee);
      tbt.putLong(0, followee);
      tbt.putLong(1, follower);
    }

    csv.close();

    TupleBatch tb = null;
    while ((tb = tbr.popAny()) != null) {
      insert(workerIDs[0], TWITTER_R, TWITTER_R_SCHEMA, tb);
    }
    while ((tb = tbs.popAny()) != null) {
      insert(workerIDs[0], TWITTER_S, TWITTER_S_SCHEMA, tb);
    }
    while ((tb = tbt.popAny()) != null) {
      insert(workerIDs[0], TWITTER_T, TWITTER_T_SCHEMA, tb);
    }

    /* import dataset to catalog */
    server.importDataset(TWITTER_R, TWITTER_R_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0])));
    server.importDataset(TWITTER_S, TWITTER_S_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0])));
    server.importDataset(TWITTER_T, TWITTER_T_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0])));

  }

  @Test
  public void twitterTriangle() throws Exception {

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    insertTwitterTables();

    /* 1. get result by a pipe-lined join */
    final DbQueryScan scan1 = new DbQueryScan(TWITTER_R, TWITTER_R_SCHEMA);
    final DbQueryScan scan2 = new DbQueryScan(TWITTER_S, TWITTER_S_SCHEMA);
    final DbQueryScan scan3 = new DbQueryScan(TWITTER_T, TWITTER_T_SCHEMA);

    final List<String> outSchema1 = ImmutableList.of("x", "y1", "y2", "z");
    final SymmetricHashJoin joinFirstStep =
        new SymmetricHashJoin(outSchema1, scan1, scan2, new int[] { 1 }, new int[] { 0 });

    final List<String> outSchema2 = ImmutableList.of("x", "y", "z");
    final SymmetricHashJoin joinSecondStep =
        new SymmetricHashJoin(outSchema2, joinFirstStep, scan3, new int[] { 0, 3 }, new int[] { 0, 1 }, new int[] {
            0, 1 }, new int[] { 1 });
    final ExchangePairID serverReceiveID = ExchangePairID.newID();
    final CollectProducer cp1 = new CollectProducer(joinSecondStep, serverReceiveID, MASTER_ID);
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { cp1 }));
    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(new SinkRoot(queueStore));

    server.submitQueryPlan("", "", "", serverPlan, workerPlans).sync();

    TupleBatchBuffer actualResult = new TupleBatchBuffer(queueStore.getSchema());
    TupleBatch tb = null;
    while (!receivedTupleBatches.isEmpty()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        tb.compactInto(actualResult);
      }
    }
    final HashMap<Tuple, Integer> pipelineJoinResultBag = TestUtils.tupleBatchToTupleBag(actualResult);

    /* 2. get result by local leapfrog join */
    File queryJson = new File("./jsonQueries/multiwayJoin_shumo/twitterTriangleJoinSystemTest.json");
    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    conn.disconnect();
    while (!server.queryCompleted(5)) {
      Thread.sleep(100);
    }

    final Schema triangleSchema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE), ImmutableList.of("x", "y", "z"));
    final RelationKey triangleKey = RelationKey.of("test", "triangleJoin", "result");
    final ExchangePairID serverReceiveIDMJ = ExchangePairID.newID();
    final DbQueryScan scan = new DbQueryScan(triangleKey, triangleSchema);
    final CollectProducer cp2 = new CollectProducer(scan, serverReceiveIDMJ, MASTER_ID);
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlansMJ = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlansMJ.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { cp2 }));

    final CollectConsumer serverCollectMJ =
        new CollectConsumer(cp2.getSchema(), serverReceiveIDMJ, new int[] { workerIDs[0] });
    final LinkedBlockingQueue<TupleBatch> receivedMJTB = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStoreMJ = new TBQueueExporter(receivedMJTB, serverCollectMJ);
    final SingleQueryPlanWithArgs serverPlanMJ = new SingleQueryPlanWithArgs(new SinkRoot(queueStoreMJ));

    server.submitQueryPlan("", "", "", serverPlanMJ, workerPlansMJ).sync();

    TupleBatchBuffer multiwayJoinResult = new TupleBatchBuffer(queueStoreMJ.getSchema());
    tb = null;
    while (!receivedMJTB.isEmpty()) {
      tb = receivedMJTB.poll();
      if (tb != null) {
        tb.compactInto(multiwayJoinResult);
      }
    }
    final HashMap<Tuple, Integer> multiwayJoinResultBag = TestUtils.tupleBatchToTupleBag(multiwayJoinResult);

    TestUtils.assertTupleBagEqual(pipelineJoinResultBag, multiwayJoinResultBag);

  }

  @Test
  public void twoWayJoinUsingMultiwayJoinOperator() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    /* Step 1: generate test data */
    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    server.importDataset(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
        workerIDs[1])));
    server.importDataset(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
        workerIDs[1])));

    /* Step 2: submit JSON query plan */
    File queryJson = new File("./jsonQueries/multiwayJoin_shumo/twoWayJoinSystemTest.json");
    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    conn.disconnect();
    while (!server.queryCompleted(3)) {
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
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { cp1 }));
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { cp1 }));
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(new SinkRoot(queueStore));
    server.submitQueryPlan("", "", "", serverPlan, workerPlans).sync();

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

    CSVReader csv1 = new CSVReader(new FileReader(table1Worker1FilePath.toFile()));
    CSVReader csv2 = new CSVReader(new FileReader(table2Worker1FilePath.toFile()));
    CSVReader csv3 = new CSVReader(new FileReader(table1Worker2FilePath.toFile()));
    CSVReader csv4 = new CSVReader(new FileReader(table2Worker2FilePath.toFile()));

    String[] line;

    while ((line = csv1.readNext()) != null) {
      tb1w1.putLong(0, Long.parseLong(line[0]));
      tb1w1.putString(1, line[1]);
    }
    csv1.close();

    while ((line = csv2.readNext()) != null) {
      tb2w1.putLong(0, Long.parseLong(line[0]));
      tb2w1.putString(1, line[1]);
    }
    csv2.close();

    while ((line = csv3.readNext()) != null) {
      tb1w2.putLong(0, Long.parseLong(line[0]));
      tb1w2.putString(1, line[1]);
    }
    csv3.close();

    while ((line = csv4.readNext()) != null) {
      tb2w2.putLong(0, Long.parseLong(line[0]));
      tb2w2.putString(1, line[1]);
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
    server.importDataset(JOIN_TEST_TABLE_1, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
        workerIDs[1])));
    server.importDataset(JOIN_TEST_TABLE_2, JOIN_INPUT_SCHEMA, new HashSet<Integer>(Arrays.asList(workerIDs[0],
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

    final LeapFrogJoin localjoin = new LeapFrogJoin(new Operator[] { o1, o2 }, fieldMap, outputMap, outputColumnNames);
    localjoin.getSchema();
    final CollectProducer cp1 = new CollectProducer(localjoin, serverReceiveID, MASTER_ID);
    final HashMap<Integer, SingleQueryPlanWithArgs> workerPlans = new HashMap<Integer, SingleQueryPlanWithArgs>();
    workerPlans.put(workerIDs[0], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));
    workerPlans.put(workerIDs[1], new SingleQueryPlanWithArgs(new RootOperator[] { sp1, sp2, cp1 }));

    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    final SingleQueryPlanWithArgs serverPlan = new SingleQueryPlanWithArgs(new SinkRoot(queueStore));

    server.submitQueryPlan("", "", "", serverPlan, workerPlans).sync();
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
