/**
 *
 */
package edu.washington.escience.myria.systemtest;

import org.junit.Test;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.UriSource;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 * 
 */
public class TempDataDownloadTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    /* Read the data back in from S3 into one worker */
    RelationKey relationKeyDownload = RelationKey.of("public", "adhoc", "download");
    DataSource relationSourceS3 = new UriSource("s3://myria-test/test.txt");
    JsonAPIUtils.ingestData("localhost", masterDaemonPort, ingest(relationKeyDownload, relationSchema,
        relationSourceS3, ' ', new RoundRobinPartitionFunction(2)));

    String dstData =
        JsonAPIUtils.download("localhost", masterDaemonPort, relationKeyDownload.getUserName(), relationKeyDownload
            .getProgramName(), relationKeyDownload.getRelationName(), "json");
  }
}
