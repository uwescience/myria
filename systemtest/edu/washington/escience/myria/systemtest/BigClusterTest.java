package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.Timeout;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.operator.DuplicateTBGenerator;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.FixValuePartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestUtils;

@Ignore
public class BigClusterTest extends SystemTestBase {

  public BigClusterTest() {
    globalTimeout = Timeout.seconds(1200);
  }

  public static final int NUM_WORKERS;

  static {
    if (TestUtils.inTravis()) {
      NUM_WORKERS = 2;
    } else {
      NUM_WORKERS = 30;
    }
  }

  @Override
  public Map<Integer, SocketInfo> getWorkers() {
    HashMap<Integer, SocketInfo> m = new HashMap<Integer, SocketInfo>();
    Random r = new Random();
    int step = r.nextInt(5) + 1;
    for (int i = 0; i < NUM_WORKERS; i++) {
      m.put(
          MyriaConstants.MASTER_ID + step * (i + 1),
          new SocketInfo(DEFAULT_WORKER_STARTING_PORT + i));
    }
    return m;
  }

  @Override
  public Map<String, String> getMasterConfigurations() {
    HashMap<String, String> masterConfigurations = new HashMap<String, String>();
    masterConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "2");
    masterConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER, "1");
    return masterConfigurations;
  }

  @Override
  public Map<String, String> getWorkerConfigurations() {
    HashMap<String, String> workerConfigurations = new HashMap<String, String>();
    workerConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_CAPACITY, "2");
    workerConfigurations.put(MyriaSystemConfigKeys.OPERATOR_INPUT_BUFFER_RECOVER_TRIGGER, "1");
    return workerConfigurations;
  }

  @Test
  public void collectTest() throws Exception {
    // skip always, because this test destroys my computer every time
    assumeTrue(false);
    final int NUM_DUPLICATES = 100;
    final int TB_SIZE = 10;

    TupleBatch tb = TestUtils.generateRandomTuples(TB_SIZE, TB_SIZE, false).popAny();
    ;

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DuplicateTBGenerator scanTable = new DuplicateTBGenerator(tb, NUM_DUPLICATES);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final CollectProducer cp1 = new CollectProducer(scanTable, serverReceiveID, MASTER_ID);
    for (int workerID : workerIDs) {
      workerPlans.put(workerID, new RootOperator[] {cp1});
    }

    final CollectConsumer serverCollect =
        new CollectConsumer(tb.getSchema(), serverReceiveID, workerIDs);
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches =
        new LinkedBlockingQueue<TupleBatch>(10);
    final TBQueueExporter queueStore = new TBQueueExporter(receivedTupleBatches, serverCollect);
    SinkRoot serverPlan = new SinkRoot(queueStore);

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);

    int numResultTuples = 0;
    while (!qf.isDone()) {
      tb = receivedTupleBatches.poll();
      if (tb != null) {
        numResultTuples += tb.numTuples();
        System.out.println(numResultTuples);
      }
    }

    assertEquals(NUM_DUPLICATES * TB_SIZE * workerIDs.length, numResultTuples);
  }

  @Test
  public void broadcastTest() throws Exception {
    // skip always, because this test destroys my computer every time
    assumeTrue(false);
    final int NUM_DUPLICATES = 10;
    final int NUM_BROADCAST = 10;
    TupleBatch tb = TestUtils.generateRandomTuples(10, 10, false).popAny();

    final ExchangePairID[] broadcastIDs = new ExchangePairID[NUM_BROADCAST];
    for (int i = 0; i < broadcastIDs.length; i++) {
      broadcastIDs[i] = ExchangePairID.newID(); // for BroadcastOperator
    }

    int[][] broadcastPartition = new int[1][workerIDs.length];
    for (int i = 0; i < broadcastPartition.length; i++) {
      broadcastPartition[0][i] = i;
    }

    /* Set producer */
    final DuplicateTBGenerator[] scans = new DuplicateTBGenerator[NUM_BROADCAST];
    for (int i = 0; i < scans.length; i++) {
      scans[i] = new DuplicateTBGenerator(tb, NUM_DUPLICATES);
    }

    final GenericShuffleProducer[] bps = new GenericShuffleProducer[NUM_BROADCAST];
    for (int i = 0; i < bps.length; i++) {
      bps[i] =
          new GenericShuffleProducer(
              scans[i],
              broadcastIDs[i],
              broadcastPartition,
              workerIDs,
              new FixValuePartitionFunction(0));
    }

    /* Set consumer */
    final GenericShuffleConsumer[] bss = new GenericShuffleConsumer[NUM_BROADCAST];
    for (int i = 0; i < bss.length; i++) {
      bss[i] = new GenericShuffleConsumer(tb.getSchema(), broadcastIDs[i], workerIDs);
    }

    SinkRoot[] workerSinks = new SinkRoot[NUM_BROADCAST];

    for (int i = 0; i < bss.length; i++) {
      workerSinks[i] = new SinkRoot(bss[i]);
    }

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    for (int wID : workerIDs) {
      workerPlans.put(
          wID, ArrayUtils.addAll(ArrayUtils.addAll(new RootOperator[] {}, workerSinks), bps));
    }

    SinkRoot serverPlan = new SinkRoot(new EOSSource());

    QueryFuture qf = server.submitQueryPlan(serverPlan, workerPlans);
    qf.get();
    Assert.assertTrue(qf.isDone());
  }

  @Test
  public void downloadTest() throws Exception {
    // skip always, because this test destroys my computer every time
    assumeTrue(false);
    final int NUM_DUPLICATES = 1;

    URL url =
        new URL(
            String.format(
                "http://%s:%d/dataset/download_test?num_tb=%d&format=%s",
                "localhost",
                masterDaemonPort,
                NUM_DUPLICATES,
                "json"));
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setDoOutput(true);
    conn.setRequestMethod("GET");

    long start = System.nanoTime();
    if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
      throw new IOException("Failed to download result:" + conn.getResponseCode());
    }

    long numBytesRead = 0;
    try {
      InputStream is = conn.getInputStream();
      while (is.read() >= 0) {
        numBytesRead++;
      }
    } finally {
      conn.disconnect();
    }
    long nanoElapse = System.nanoTime() - start;

    System.out.println("Download size: " + (numBytesRead * 1.0 / 1024 / 1024 / 1024) + " GB\n");
    System.out.println(
        "Speed is: "
            + (numBytesRead * 1.0 / 1024 / 1024 / TimeUnit.NANOSECONDS.toSeconds(nanoElapse))
            + " MB/s");
  }
}
