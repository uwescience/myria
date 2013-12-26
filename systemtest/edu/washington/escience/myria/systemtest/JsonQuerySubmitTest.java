package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.util.JsonAPIUtils;

public class JsonQuerySubmitTest extends SystemTestBase {

  static Logger LOGGER = LoggerFactory.getLogger(JsonQuerySubmitTest.class);

  @Override
  public Map<Integer, SocketInfo> getWorkers() {
    HashMap<Integer, SocketInfo> m = new HashMap<Integer, SocketInfo>();
    m.put(1, new SocketInfo(DEFAULT_WORKER_STARTING_PORT));
    m.put(2, new SocketInfo(DEFAULT_WORKER_STARTING_PORT + 1));
    return m;
  }

  private static final String emptyIngestJson = "{" + "  \"relation_key\" : {" + "    \"user_name\" : \"public\","
      + "    \"program_name\" : \"adhoc\"," + "    \"relation_name\" : \"smallTable\"" + "  }," + "  \"schema\" : {"
      + "    \"column_types\" : [\"STRING_TYPE\", \"LONG_TYPE\"]," + "    \"column_names\" : [\"foo\", \"bar\"]"
      + "  }," + "\"source\" : { \"data_type\" : \"Empty\" } " + "}";

  @Test
  public void emptySubmitTest() throws Exception {

    HttpURLConnection conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, emptyIngestJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    conn.disconnect();
  }

  @Test
  public void datasetPutTest() throws Exception {

    HttpURLConnection conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, emptyIngestJson);
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

    String data = JsonAPIUtils.download("localhost", masterDaemonPort, "jwang", "global_join", "smallTable", "json");
    String subStr = "{\"follower\":46,\"followee\":17}";
    assertTrue(data.contains(subStr));

    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    conn.disconnect();
  }

  private String getContents(HttpURLConnection conn) {
    /* If there was any content returned, get it. */
    String content = null;
    try {
      InputStream is = conn.getInputStream();
      if (is != null) {
        content = ByteString.readFrom(is).toStringUtf8();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    /* If there was any error returned, get it. */
    String error = null;
    try {
      InputStream is = conn.getErrorStream();
      if (is != null) {
        error = ByteString.readFrom(is).toStringUtf8();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    StringBuilder ret = new StringBuilder();
    if (content != null) {
      ret.append("Content:\n").append(content);
    }
    if (error != null) {
      ret.append("Error:\n").append(error);
    }
    return ret.toString();
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

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestB0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    while (!server.queryCompleted(2)) {
      Thread.sleep(100);
    }

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestC0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    while (!server.queryCompleted(3)) {
      Thread.sleep(100);
    }

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestR);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    while (!server.queryCompleted(4)) {
      Thread.sleep(100);
    }

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
    Long result = server.getQueryResult(5);
    assertNotNull(result);
    assertEquals(result.longValue(), 4121l);
  }
}
