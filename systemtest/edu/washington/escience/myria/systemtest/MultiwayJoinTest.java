package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
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
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.SymmetricHashJoin;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.parallel.CollectConsumer;
import edu.washington.escience.myria.parallel.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
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

    String twitterFilePath =
        getClass().getClassLoader().getResource("./").getPath() + "../../testdata/twitter/TwitterK.csv";

    CSVReader csv = new CSVReader(new FileReader(twitterFilePath));

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
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    final CollectConsumer serverCollect =
        new CollectConsumer(cp1.getSchema(), serverReceiveID, new int[] { workerIDs[0] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    server.submitQueryPlan(serverPlan, workerPlans).sync();

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
    final HashMap<Integer, RootOperator[]> workerPlansMJ = new HashMap<Integer, RootOperator[]>();
    workerPlansMJ.put(workerIDs[0], new RootOperator[] { cp2 });

    final CollectConsumer serverCollectMJ =
        new CollectConsumer(cp2.getSchema(), serverReceiveIDMJ, new int[] { workerIDs[0] });
    final LinkedBlockingQueue<TupleBatch> receivedMJTB = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStoreMJ = new TBQueueExporter(receivedMJTB, serverCollectMJ);
    SinkRoot serverPlanMJ = new SinkRoot(queueStoreMJ);

    server.submitQueryPlan(serverPlanMJ, workerPlansMJ).sync();

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
    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp1 });
    final CollectConsumer serverCollect =
        new CollectConsumer(outputSchema, serverReceiveID, new int[] { workerIDs[0], workerIDs[1] });
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>();
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);
    server.submitQueryPlan(serverPlan, workerPlans).sync();

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

}
