package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.DuplicateTBGenerator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.operator.TBQueueExporter;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestUtils;

public class BigDataTest extends SystemTestBase {

  @Rule
  public TestRule globalTimeout = new Timeout(1200 * 1000);

  @Test
  public void bigCollectTest() throws Exception {

    final int NUM_DUPLICATES = 10000;

    final String[] names = TestUtils.randomFixedLengthNumericString(1000, 1005, TupleBatch.BATCH_SIZE, 20);
    final long[] ids = TestUtils.randomLong(1000, 1005, names.length);

    final Schema schema =
        new Schema(ImmutableList.of(Type.LONG_TYPE, Type.STRING_TYPE), ImmutableList.of("id", "name"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < names.length; i++) {
      tbb.putLong(0, ids[i]);
      tbb.putString(1, names[i]);
    }

    TupleBatch tb = tbb.popAny();

    final ExchangePairID serverReceiveID = ExchangePairID.newID();

    final DuplicateTBGenerator scanTable = new DuplicateTBGenerator(tb, NUM_DUPLICATES);

    final HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    final CollectProducer cp1 = new CollectProducer(scanTable, serverReceiveID, MASTER_ID);
    workerPlans.put(workerIDs[0], new RootOperator[] { cp1 });
    workerPlans.put(workerIDs[1], new RootOperator[] { cp1 });

    final CollectConsumer serverCollect = new CollectConsumer(schema, serverReceiveID, workerIDs);
    final LinkedBlockingQueue<TupleBatch> receivedTupleBatches = new LinkedBlockingQueue<TupleBatch>(10);
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

    assertEquals(NUM_DUPLICATES * TupleBatch.BATCH_SIZE * 2, numResultTuples);

  }

  @Test
  public void bigDownloadTest() throws Exception {

    final int NUM_DUPLICATES = 2000;

    URL url =
        new URL(String.format("http://%s:%d/dataset/download_test?num_tb=%d&format=%s", "localhost", masterDaemonPort,
            NUM_DUPLICATES, "json"));
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
    System.out.println("Download size: " + (numBytesRead * 1.0 / 1024 / 1024 / 1024) + " GB");
    System.out.println("Speed is: " + (numBytesRead * 1.0 / 1024 / 1024 / TimeUnit.NANOSECONDS.toSeconds(nanoElapse))
        + " MB/s");
  }
}
