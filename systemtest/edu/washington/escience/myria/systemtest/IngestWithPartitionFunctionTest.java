package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myria.accessmethod.SQLiteInfo;
import edu.washington.escience.myria.coordinator.catalog.WorkerCatalog;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.JsonAPIUtils;

public class IngestWithPartitionFunctionTest extends SystemTestBase {

  /* The path to the json files. */
  private static final String jsonTestPath = "./jsonQueries/partition_vaspol/";
  /* The filename for ingest json file without specifying the partition function. */
  private static final String ingestNotPartitionedFilename = "ingest_data.json";
  /* The filename for ingest json file with the partition function. */
  private static final String ingestPartitionedFilename = "ingest_data_partitioned.json";
  /* The filename of the query for repartitioning the data. */
  private static final String repartitionQueryFilename = "repartition.json";
  /* The myria server name. */
  private static final String serverName = "localhost";

  static Logger LOGGER = LoggerFactory.getLogger(JsonQuerySubmitTest.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @Override
  public Map<Integer, SocketInfo> getWorkers() {
    HashMap<Integer, SocketInfo> m = new HashMap<Integer, SocketInfo>();
    m.put(1, new SocketInfo(DEFAULT_WORKER_STARTING_PORT));
    m.put(2, new SocketInfo(DEFAULT_WORKER_STARTING_PORT + 1));
    return m;
  }

  /*
   * Test that ingesting using the partition function gives the same result as ingesting round robin, then repartition
   * the data.
   */
  @Test
  public void testIngestDataWithPartitionFunction() throws Exception {
    File ingestWithPartitionFuncntion = new File(jsonTestPath + ingestPartitionedFilename);
    File ingestWithoutPartitionFuncntion = new File(jsonTestPath + ingestNotPartitionedFilename);

    /*
     * (1) ingest the data in two modes with and without the partition function specified.
     * 
     * Ingest the data with the partition function specified.
     */
    HttpURLConnection conn;
    conn = JsonAPIUtils.ingestData(serverName, masterDaemonPort, ingestWithPartitionFuncntion);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    /* make sure the ingestion is done. */
    while (!server.queryCompleted(1)) {
      Thread.sleep(100);
    }
    conn.disconnect();

    /* Ingest the data without the partition function specfied. */
    conn = JsonAPIUtils.ingestData(serverName, masterDaemonPort, ingestWithoutPartitionFuncntion);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_CREATED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    /* make sure the ingestion is done. */
    while (!server.queryCompleted(1)) {
      Thread.sleep(100);
    }
    conn.disconnect();

    /* (2) submit a query to the server to re-partition the non-partitioned data and materialize it. */
    File repartitionQuery = new File(jsonTestPath + repartitionQueryFilename);
    conn = JsonAPIUtils.submitQuery(serverName, masterDaemonPort, repartitionQuery);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    LOGGER.info(getContents(conn));
    /* make sure the repartition is done. */
    while (!server.queryCompleted(1)) {
      Thread.sleep(100);
    }

    /*
     * (3) open the database and get examine the data. The data on each worker after the repartitioning should be the
     * same.
     */

    // Create the relation keys and schema as specified in the ingest file.
    final RelationKey initiallyPartitioned = RelationKey.of("vaspol", "partition_test", "partitioned");
    final RelationKey repartitioned = RelationKey.of("vaspol", "partition_test", "repartitioned");
    // Must do an ORDER BY here, in case the data queried out is in a different order.
    final String queryInitiallyPartitioned =
        "SELECT * FROM " + initiallyPartitioned.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " ORDER BY data;";
    final String queryRepartitioned =
        "SELECT * FROM " + repartitioned.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE) + " ORDER BY data;";
    final Schema schema = Schema.of(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("data"));

    // Query out the data from each of the worker and make sure that the contents are the same.
    for (int workerId : workerIDs) {
      String workerDir = getWorkerFolder(workerId);
      WorkerCatalog wc = WorkerCatalog.open(FilenameUtils.concat(workerDir, "worker.catalog"));
      SQLiteInfo sqliteWorkerOneInfo =
          (SQLiteInfo) ConnectionInfo.of(MyriaConstants.STORAGE_SYSTEM_SQLITE, wc
              .getConfigurationValue(MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_CONN_INFO));
      Iterator<TupleBatch> tupleBatchIterPartitioned =
          SQLiteAccessMethod.tupleBatchIteratorFromQuery(sqliteWorkerOneInfo, queryInitiallyPartitioned, schema);
      Iterator<TupleBatch> tupleBatchIterRepartitioned =
          SQLiteAccessMethod.tupleBatchIteratorFromQuery(sqliteWorkerOneInfo, queryRepartitioned, schema);
      while (tupleBatchIterPartitioned.hasNext()) {
        TupleBatch expected = tupleBatchIterRepartitioned.next();
        TupleBatch testing = tupleBatchIterPartitioned.next();
        assertEquals(expected.numTuples(), testing.numTuples()); // make sure that there are equal numbers of tuples.
        for (int i = 0; i < expected.numTuples(); ++i) {
          assertEquals(expected.getInt(0, i), testing.getInt(0, i));
        }
      }
      assertFalse(tupleBatchIterRepartitioned.hasNext()); // So that we are sure that the two iterators have the same
                                                          // number of tuple batches.
    }
  }
}
