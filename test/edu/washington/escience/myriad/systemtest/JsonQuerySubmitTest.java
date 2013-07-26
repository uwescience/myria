package edu.washington.escience.myriad.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.net.HttpURLConnection;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.IOUtils;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.util.JsonAPIUtils;

public class JsonQuerySubmitTest extends SystemTestBase {

  static Logger LOGGER = LoggerFactory.getLogger(JsonQuerySubmitTest.class);

  @Test
  public void jsonQuerySubmitTest() throws Exception {
    // DeploymentUtils.ensureMasterStart("localhost", masterDaemonPort);

    File queryJson = new File("./jsonQueries/sample_queries/single_join.json");
    File ingestJson = new File("./jsonQueries/globalJoin_jwang/ingest_smallTable.json");

    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    assertNotEquals(conn.getResponseCode(), HttpURLConnection.HTTP_OK);
    LOGGER.error(new String(IOUtils.readFully(conn.getErrorStream(), -1, true)));

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));

    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
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
      throw new IllegalStateException(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    /* make sure the ingesting is done. */
    Thread.sleep(200);

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestB0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    Thread.sleep(200);

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestC0);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    Thread.sleep(200);

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestR);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    Thread.sleep(200);

    File queryJson = new File("./jsonQueries/multiIDB_jwang/joinChain.json");
    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(new String(IOUtils.readFully(conn.getInputStream(), -1, true)));

    while (!server.allQueriesCompleted()) {
      Thread.sleep(100);
    }
    Long result = server.getQueryResult(5);
    Preconditions.checkNotNull(result);
    Preconditions.checkArgument(result == 4121);
  }
}
