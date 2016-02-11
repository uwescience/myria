/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;

import org.junit.Test;

import edu.washington.escience.myria.PostgresBinaryTupleWriter;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.io.UriSink;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 */
public class DataSinkTest extends SystemTestBase {

  @Test
  public void s3UploadTest() throws Exception {
    /* Ingest test data */
    DataSource relationSource = new FileSource(Paths.get("testdata", "filescan", "simple_two_col_int.txt").toString());
    RelationKey relationKey = RelationKey.of("public", "adhoc", "testIngest");
    Schema relationSchema = Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE);
    JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingest(relationKey, relationSchema, relationSource, ' ',
        new RoundRobinPartitionFunction(workerIDs.length)));

    /* Construct the query programmatically -- for the workers */
    DbQueryScan dbScan = new DbQueryScan(relationKey, relationSchema);
    DataSink sink = new UriSink("s3://myria-test/");
    DataOutput dataOutput = new DataOutput(dbScan, new PostgresBinaryTupleWriter(), sink);

    // finish the query plan
    // server.submitQueryPlan(serverPlan, workerPlans).get();

    /* Read the data back in from S3 and verify results */
    String data = ""; // S3 download via JsonUtils?
    String expectedData = "";
    assertEquals(data, expectedData);
  }
}
