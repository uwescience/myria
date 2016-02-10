/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.net.HttpURLConnection;
import java.nio.file.Paths;

import org.junit.Test;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 * 
 */
public class DataSinkTest extends SystemTestBase {

  @Test
  public void dataSink() throws Exception {
    /* Ingest test data */
    DataSource relationSource = new FileSource(Paths.get("testdata", "filescan", "simple_two_col_int.txt").toString());
    RelationKey relationKey = RelationKey.of("public", "adhoc", "testIngest");
    Schema relationSchema = Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE);
    JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingest(relationKey, relationSchema, relationSource, ' ',
        new RoundRobinPartitionFunction(workerIDs.length)));

    /* Run a Query Plan with Persist encoding */
    File queryJson = new File("./jsonQueries/dataSink_jortiz/datasink_encoding_test.json");
    HttpURLConnection conn = JsonAPIUtils.submitQuery("localhost", masterDaemonPort, queryJson);
    if (null != conn.getErrorStream()) {
      throw new IllegalStateException(getContents(conn));
    }
    assertEquals(HttpURLConnection.HTTP_ACCEPTED, conn.getResponseCode());
    long queryId = getQueryStatus(conn).queryId;
    conn.disconnect();
    while (!server.getQueryManager().queryCompleted(queryId)) {
      Thread.sleep(5);
    }
    QueryStatusEncoding status = server.getQueryManager().getQueryStatus(queryId);
    assertEquals(QueryStatusEncoding.Status.SUCCESS, status.status);
  }

}
