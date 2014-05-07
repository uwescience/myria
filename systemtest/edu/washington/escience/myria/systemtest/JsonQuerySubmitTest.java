package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

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
import edu.washington.escience.myria.io.EmptySource;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.util.JsonAPIUtils;

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
    DatasetEncoding emptyIngest = new DatasetEncoding();
    emptyIngest.relationKey = RelationKey.of("public", "adhoc", "smallTable");
    emptyIngest.schema = Schema.of(ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE), ImmutableList.of("foo", "bar"));
    emptyIngest.source = new EmptySource();
    return MyriaJsonMapperProvider.getWriter().writeValueAsString(emptyIngest);
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
    while (!server.queryCompleted(1)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(1).status);

    String data = JsonAPIUtils.download("localhost", masterDaemonPort, "jwang", "global_join", "smallTable", "json");
    String subStr = "{\"follower\":46,\"followee\":17}";
    assertTrue(data.contains(subStr));
    while (!server.queryCompleted(2)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(2).status);

    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    conn.disconnect();
    while (!server.queryCompleted(3)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(3).status);
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
    LOGGER.info(getContents(conn));
    /* make sure the ingestion is done. */
    while (!server.queryCompleted(1)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(1).status);

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestB0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    while (!server.queryCompleted(2)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(2).status);

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestC0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    while (!server.queryCompleted(3)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(3).status);

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestR);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    while (!server.queryCompleted(4)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(4).status);

    File queryJson = new File("./jsonQueries/multiIDB_jwang/joinChain.json");
    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    while (!server.queryCompleted(5)) {
      Thread.sleep(100);
    }
    assertEquals(QueryStatusEncoding.Status.SUCCESS, server.getQueryStatus(5).status);
  }
}
