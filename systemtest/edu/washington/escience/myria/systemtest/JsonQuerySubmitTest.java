package edu.washington.escience.myria.systemtest;

import static org.assertj.core.api.Assertions.assertThat;
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

import org.apache.commons.httpclient.HttpStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.EmptyRelationEncoding;
import edu.washington.escience.myria.api.encoding.LocalMultiwayConsumerEncoding;
import edu.washington.escience.myria.api.encoding.LocalMultiwayProducerEncoding;
import edu.washington.escience.myria.api.encoding.PlanFragmentEncoding;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.api.encoding.SingletonEncoding;
import edu.washington.escience.myria.api.encoding.SinkRootEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubQueryEncoding;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.EmptySource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
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
    Schema schema =
        Schema.of(
            ImmutableList.of(Type.STRING_TYPE, Type.LONG_TYPE), ImmutableList.of("foo", "bar"));
    return ingest(key, schema, new EmptySource(), null, null);
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
    JsonAPIUtils.replace(
        "localhost", masterDaemonPort, "public", "adhoc", "smallTable", dataset, "csv");

    String fetchedDataset =
        JsonAPIUtils.download(
            "localhost", masterDaemonPort, "public", "adhoc", "smallTable", "csv");
    assertTrue(fetchedDataset.contains("pizza pizza"));

    // Replace the dataset with all new contents
    dataset = "mexico\t42\n" + "sri lanka\t12342\n" + "belize\t802304";
    JsonAPIUtils.replace(
        "localhost", masterDaemonPort, "public", "adhoc", "smallTable", dataset, "tsv");

    fetchedDataset =
        JsonAPIUtils.download(
            "localhost", masterDaemonPort, "public", "adhoc", "smallTable", "csv");
    assertFalse(fetchedDataset.contains("pizza pizza"));
    assertTrue(fetchedDataset.contains("sri lanka"));
  }

  @Test
  public void ingestTest() throws Exception {
    /* good ingestion. */
    DataSource source =
        new FileSource(Paths.get("testdata", "filescan", "simple_two_col_int.txt").toString());
    RelationKey key = RelationKey.of("public", "adhoc", "testIngest");
    Schema schema =
        Schema.of(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("x", "y"));
    Character delimiter = ' ';
    HttpURLConnection conn =
        JsonAPIUtils.ingestData(
            "localhost", masterDaemonPort, ingest(key, schema, source, delimiter, null));
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    DatasetStatus status = getDatasetStatus(conn);
    assertEquals(7, status.getNumTuples());
    assertEquals(2, status.getHowPartitioned().getWorkers().size());
    PartitionFunction pf = status.getHowPartitioned().getPf();
    /* not specified, should be RoundRobin. */
    assertTrue(pf instanceof RoundRobinPartitionFunction);
    assertEquals(2, pf.numPartition());
    conn.disconnect();
    /* bad ingestion. */
    delimiter = ',';
    RelationKey newkey = RelationKey.of("public", "adhoc", "testbadIngest");
    conn =
        JsonAPIUtils.ingestData(
            "localhost", masterDaemonPort, ingest(newkey, schema, source, delimiter, null));
    assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_INTERNAL_ERROR);
    conn.disconnect();

    /* hash-partitioned ingest. */
    delimiter = ' ';
    RelationKey keyP = RelationKey.of("public", "adhoc", "testIngestHashPartitioned");
    conn =
        JsonAPIUtils.ingestData(
            "localhost",
            masterDaemonPort,
            ingest(keyP, schema, source, delimiter, new SingleFieldHashPartitionFunction(null, 1)));
    assertEquals(conn.getResponseCode(), HttpURLConnection.HTTP_CREATED);
    status = getDatasetStatus(conn);
    pf = status.getHowPartitioned().getPf();
    /* specified, should be SingleField with index = 1. */
    assertEquals(2, pf.numPartition());
    assertTrue(pf instanceof SingleFieldHashPartitionFunction);
    assertEquals(1, ((SingleFieldHashPartitionFunction) pf).getIndex());
    conn.disconnect();
  }

  @Test
  public void jsonQuerySubmitTest() throws Exception {
    // DeploymentUtils.ensureMasterStart("localhost", masterDaemonPort);
    File queryJson = new File("./jsonQueries/sample_queries/single_join.json");
    File ingestJson = new File("./jsonQueries/globalJoin_jwang/ingest_smallTable.json");

    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(5);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(QueryStatusEncoding.Status.ERROR, status.status);
    assertThat(status.message).contains("Unable to find workers");

    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    DatasetStatus datasetStatus = getDatasetStatus(conn);
    PartitionFunction pf = datasetStatus.getHowPartitioned().getPf();
    assertEquals(2, pf.numPartition());
    assertTrue(pf instanceof SingleFieldHashPartitionFunction);
    assertEquals(0, ((SingleFieldHashPartitionFunction) pf).getIndex());
    conn.disconnect();

    String data =
        JsonAPIUtils.download(
            "localhost", masterDaemonPort, "jwang", "global_join", "smallTable", "json");
    String subStr = "{\"follower\":46,\"followee\":17}";
    assertTrue(data.contains(subStr));

    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(5);
    }
    status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(QueryStatusEncoding.Status.SUCCESS, status.status);
    assertTrue(status.language.equals("datalog"));
    assertEquals(status.ftMode, FTMode.NONE);
    assertEquals(status.profilingMode.size(), 0);
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
    assertEquals(
        QueryStatusEncoding.Status.SUCCESS,
        server.getQueryManager().getQueryStatus(getDatasetStatus(conn).getQueryId()).status);
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
    assertEquals(
        QueryStatusEncoding.Status.SUCCESS,
        server.getQueryManager().getQueryStatus(queryId).status);
  }

  @Test
  public void abortedDownloadTest() throws Exception {
    // skip in travis
    TestUtils.skipIfInTravis();
    final int NUM_DUPLICATES = 2000;
    final int BYTES_TO_READ = 1024; // read 1 kb

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
        if (numBytesRead >= BYTES_TO_READ) {
          break;
        }
      }
    } finally {
      conn.disconnect();
    }
    long nanoElapse = System.nanoTime() - start;
    System.out.println("Download size: " + (numBytesRead * 1.0 / 1024 / 1024 / 1024) + " GB");
    System.out.println(
        "Speed is: "
            + (numBytesRead * 1.0 / 1024 / 1024 / TimeUnit.NANOSECONDS.toSeconds(nanoElapse))
            + " MB/s");
    while (server.getQueryManager().getQueries(1L, null, null, null).get(0).finishTime == null) {
      Thread.sleep(100);
    }
    QueryStatusEncoding qs = server.getQueryManager().getQueries(1L, null, null, null).get(0);
    assertTrue(qs.status == QueryStatusEncoding.Status.ERROR);
  }

  @Test
  public void multiwayBetweenSingletonTest693() throws Exception {
    int opId = 0;

    // Source
    SingletonEncoding singleton = new SingletonEncoding();
    singleton.opId = opId++;
    LocalMultiwayProducerEncoding prod1 = new LocalMultiwayProducerEncoding();
    prod1.argChild = singleton.opId;
    prod1.opId = opId++;
    // Sink after split
    LocalMultiwayConsumerEncoding cons1 = new LocalMultiwayConsumerEncoding();
    cons1.argOperatorId = prod1.opId;
    cons1.opId = opId++;
    SinkRootEncoding sink = new SinkRootEncoding();
    sink.argChild = cons1.opId;
    sink.opId = opId++;

    PlanFragmentEncoding frag1 = PlanFragmentEncoding.of(singleton, prod1);
    PlanFragmentEncoding frag2 = PlanFragmentEncoding.of(cons1, sink);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag1, frag2));
    query.logicalRa = "Valid query with single split test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);
  }

  @Test
  public void severalMultiwayBetweenSingletonTest() throws Exception {
    int opId = 0;

    // Source
    SingletonEncoding singleton = new SingletonEncoding();
    singleton.opId = opId++;
    LocalMultiwayProducerEncoding prod1 = new LocalMultiwayProducerEncoding();
    prod1.argChild = singleton.opId;
    prod1.opId = opId++;
    // Intermediate split
    LocalMultiwayConsumerEncoding cons1 = new LocalMultiwayConsumerEncoding();
    cons1.argOperatorId = prod1.opId;
    cons1.opId = opId++;
    LocalMultiwayProducerEncoding prod2 = new LocalMultiwayProducerEncoding();
    prod2.argChild = cons1.opId;
    prod2.opId = opId++;
    // Sink after second split
    LocalMultiwayConsumerEncoding cons2 = new LocalMultiwayConsumerEncoding();
    cons2.argOperatorId = prod2.opId;
    cons2.opId = opId++;
    SinkRootEncoding sink = new SinkRootEncoding();
    sink.argChild = cons2.opId;
    sink.opId = opId++;

    PlanFragmentEncoding frag1 = PlanFragmentEncoding.of(singleton, prod1);
    PlanFragmentEncoding frag2 = PlanFragmentEncoding.of(cons1, prod2);
    PlanFragmentEncoding frag3 = PlanFragmentEncoding.of(cons2, sink);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag1, frag2, frag3));
    query.logicalRa = "Valid query with multiple splits test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);
  }

  @Test
  public void severalMultiwayBetweenSingletonTestNewOpIds() throws Exception {
    int opId = 10;

    // Source
    SingletonEncoding singleton = new SingletonEncoding();
    singleton.opId = opId++;
    LocalMultiwayProducerEncoding prod1 = new LocalMultiwayProducerEncoding();
    prod1.argChild = singleton.opId;
    prod1.opId = opId++;
    // Intermediate split
    LocalMultiwayConsumerEncoding cons1 = new LocalMultiwayConsumerEncoding();
    cons1.argOperatorId = prod1.opId;
    cons1.opId = opId++;
    LocalMultiwayProducerEncoding prod2 = new LocalMultiwayProducerEncoding();
    prod2.argChild = cons1.opId;
    prod2.opId = opId++;
    // Sink after second split
    LocalMultiwayConsumerEncoding cons2 = new LocalMultiwayConsumerEncoding();
    cons2.argOperatorId = prod2.opId;
    cons2.opId = opId++;
    LocalMultiwayProducerEncoding prod3 = new LocalMultiwayProducerEncoding();
    prod3.argChild = cons2.opId;
    prod3.opId = opId++;
    // Sink after second split
    LocalMultiwayConsumerEncoding cons3 = new LocalMultiwayConsumerEncoding();
    cons3.argOperatorId = prod3.opId;
    cons3.opId = opId++;
    LocalMultiwayProducerEncoding prod4 = new LocalMultiwayProducerEncoding();
    prod4.argChild = cons3.opId;
    prod4.opId = opId++;
    // Sink after second split
    LocalMultiwayConsumerEncoding cons4 = new LocalMultiwayConsumerEncoding();
    cons4.argOperatorId = prod4.opId;
    cons4.opId = opId++;
    SinkRootEncoding sink = new SinkRootEncoding();
    sink.argChild = cons4.opId;
    sink.opId = opId++;

    PlanFragmentEncoding frag1 = PlanFragmentEncoding.of(singleton, prod1);
    PlanFragmentEncoding frag2 = PlanFragmentEncoding.of(cons1, prod2);
    PlanFragmentEncoding frag3 = PlanFragmentEncoding.of(cons2, prod3);
    PlanFragmentEncoding frag4 = PlanFragmentEncoding.of(cons3, prod4);
    PlanFragmentEncoding frag5 = PlanFragmentEncoding.of(cons4, sink);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag1, frag2, frag3, frag4, frag5));
    query.logicalRa = "Valid query with multiple splits test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(status.message, Status.SUCCESS, status.status);
  }

  @Test
  public void fragmentNoRootTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(empty);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "Fragment no root test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(Status.ERROR, status.status);
    assertThat(status.message).contains("No RootOperator detected");
  }

  @Test
  public void fragmentTwoRootsTest() throws Exception {
    EmptyRelationEncoding empty = new EmptyRelationEncoding();
    SinkRootEncoding sink1 = new SinkRootEncoding();
    SinkRootEncoding sink2 = new SinkRootEncoding();
    empty.opId = 0;
    empty.schema = Schema.ofFields("x", Type.LONG_TYPE);
    sink1.opId = 1;
    sink1.argChild = empty.opId;
    sink2.opId = 2;
    sink2.argChild = empty.opId;
    PlanFragmentEncoding frag = PlanFragmentEncoding.of(empty, sink1, sink2);

    QueryEncoding query = new QueryEncoding();
    query.plan = new SubQueryEncoding(ImmutableList.of(frag));
    query.logicalRa = "Fragment no root test";
    query.rawQuery = query.logicalRa;

    HttpURLConnection conn = submitQuery(query);
    assertEquals(HttpStatus.SC_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(1);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(Status.ERROR, status.status);
    assertThat(status.message).contains("Multiple RootOperator detected");
  }

  @Test
  public void jsonTestNoNullChild() throws Exception {
    File ingestJson = new File("./jsonQueries/globalJoin_jwang/ingest_smallTable.json");
    File queryJson = new File("./jsonQueries/nullChild_jortiz/ThreeWayLocalJoin.json");

    /* ingest small table data */
    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    conn = JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingestJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());

    /* run the query */
    conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(5);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(Status.SUCCESS, status.status);
  }
}
