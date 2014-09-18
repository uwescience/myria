package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.DatasetEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubQueryEncoding;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.EmptySource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.util.JsonAPIUtils;
import edu.washington.escience.myria.util.TestUtils;

public class JsonQuerySubmitTest extends SystemTestBase {

  static Logger LOGGER = LoggerFactory.getLogger(JsonQuerySubmitTest.class);

  /**
   * Construct an empty ingest request.
   * 
   * @return a request to ingest an empty dataset called "public:adhoc:smallTable"
   * @throws JsonProcessingException if there is an error producing the JSON
   */
  public static String emptyIngest() throws JsonProcessingException {
    /* Construct the JSON for an Empty Ingest request. */
    RelationKey key = RelationKey.of("public", "adhoc", "smallTable");
    Schema schema = Schema.of(ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE), ImmutableList.of("foo", "bar"));
    return ingest(key, schema, new EmptySource(), null);
  }

  public static String ingest(RelationKey key, Schema schema, DataSource source, @Nullable Character delimiter)
      throws JsonProcessingException {
    DatasetEncoding ingest = new DatasetEncoding();
    ingest.relationKey = key;
    ingest.schema = schema;
    ingest.source = source;
    if (delimiter != null) {
      ingest.delimiter = delimiter;
    }
    return MyriaJsonMapperProvider.getWriter().writeValueAsString(ingest);
  }

  @Override
  public Map<Integer, SocketInfo> getWorkers() {
    HashMap<Integer, SocketInfo> m = new HashMap<Integer, SocketInfo>();
    m.put(1, new SocketInfo(DEFAULT_WORKER_STARTING_PORT));
    m.put(2, new SocketInfo(DEFAULT_WORKER_STARTING_PORT + 1));
    return m;
  }

  @Test
  public void emptySubmitTest() throws Exception {
    HttpURLConnection conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, emptyIngest());
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    conn.disconnect();
  }

  @Test
  public void datasetPutTest() throws Exception {

    HttpURLConnection conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, emptyIngest());
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    conn.disconnect();

    String dataset = "Hello world,3242\n" + "goodbye world,321\n" + "pizza pizza,104";
    JsonAPIUtils.replace("localhost", masterDaemonPort, "public", "adhoc", "smallTable", dataset, "csv");

    String fetchedDataset =
        JsonAPIUtils.download("localhost", masterDaemonPort, "public", "adhoc", "smallTable", "csv");
    assertTrue(fetchedDataset.contains("pizza pizza"));

    // Replace the dataset with all new contents
    dataset = "mexico\t42\n" + "sri lanka\t12342\n" + "belize\t802304";
    JsonAPIUtils.replace("localhost", masterDaemonPort, "public", "adhoc", "smallTable", dataset, "tsv");

    fetchedDataset = JsonAPIUtils.download("localhost", masterDaemonPort, "public", "adhoc", "smallTable", "csv");
    assertFalse(fetchedDataset.contains("pizza pizza"));
    assertTrue(fetchedDataset.contains("sri lanka"));
  }

  @Test
  public void ingestTest() throws Exception {
    /* good ingestion. */
    DataSource source = new FileSource(Paths.get("testdata", "filescan", "simple_two_col_int.txt").toString());
    RelationKey key = RelationKey.of("public", "adhoc", "testIngest");
    Schema schema = Schema.of(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("x", "y"));
    Character delimiter = ' ';
    HttpURLConnection conn =
        JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingest(key, schema, source, delimiter));
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_CREATED);
    assertEquals(getDatasetStatus(conn).getNumTuples(), 7);
    conn.disconnect();
    /* bad ingestion. */
    delimiter = ',';
    RelationKey newkey = RelationKey.of("public", "adhoc", "testbadIngest");
    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingest(newkey, schema, source, delimiter));
    assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_INTERNAL_ERROR);
    conn.disconnect();
  }

  @Test
  public void jsonQuerySubmitTest() throws Exception {
    // DeploymentUtils.ensureMasterStart("localhost", masterDaemonPort);
    File queryJson = new File("./jsonQueries/sample_queries/single_join.json");
    File ingestJson = new File("./jsonQueries/globalJoin_jwang/ingest_smallTable.json");

    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_BAD_REQUEST);
    conn.disconnect();

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    conn.disconnect();

    String data = JsonAPIUtils.download("localhost", masterDaemonPort, "jwang", "global_join", "smallTable", "json");
    String subStr = "{\"follower\":46,\"followee\":17}";
    assertTrue(data.contains(subStr));

    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(100);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(QueryStatusEncoding.Status.SUCCESS, status.status);
    assertTrue(status.language.equals("datalog"));
    assertTrue(status.ftMode.equals("none"));
    assertFalse(status.profilingMode);
    assertTrue(status.plan instanceof SubQueryEncoding);
    assertEquals(((SubQueryEncoding) status.plan).fragments.size(), 3);
  }

  @Test
  public void joinChainResultTest() throws Exception {
    // DeploymentUtils.ensureMasterStart("localhost", masterDaemonPort);

    File ingestA0 = new File("./jsonQueries/multiIDB_jwang/ingest_a0.json");
    File ingestB0 = new File("./jsonQueries/multiIDB_jwang/ingest_b0.json");
    File ingestC0 = new File("./jsonQueries/multiIDB_jwang/ingest_c0.json");
    File ingestR = new File("./jsonQueries/multiIDB_jwang/ingest_r.json");

    HttpURLConnection conn;
    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestA0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    conn.disconnect();

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestB0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    conn.disconnect();

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestC0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    conn.disconnect();

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestR);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryManager().getQueryStatus(
        getDatasetStatus(conn).getQueryId()).status);
    conn.disconnect();

    File queryJson = new File("./jsonQueries/multiIDB_jwang/joinChain.json");
    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryManager().getQueryStatus(queryId).status);
  }

  @Test
  public void abortedDownloadTest() throws Exception {
    // skip in travis
    if (TestUtils.inTravis()) {
      return;
    }
    final int NUM_DUPLICATES = 2000;
    final int BYTES_TO_READ = 1024; // read 1 kb

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
        if (numBytesRead >= BYTES_TO_READ) {
          break;
        }
      }
    } finally {
      conn.disconnect();
    }
    long nanoElapse = System.nanoTime() - start;
    System.out.println("Download size: " + (numBytesRead * 1.0 / 1024 / 1024 / 1024) + " GB");
    System.out.println("Speed is: " + (numBytesRead * 1.0 / 1024 / 1024 / TimeUnit.NANOSECONDS.toSeconds(nanoElapse))
        + " MB/s");
    while (server.getQueryManager().getQueries(1, 0).get(0).finishTime == null) {
      Thread.sleep(100);
    }
    QueryStatusEncoding qs = server.getQueryManager().getQueries(1, 0).get(0);
    assertTrue(qs.status == QueryStatusEncoding.Status.ERROR);
  }
}
