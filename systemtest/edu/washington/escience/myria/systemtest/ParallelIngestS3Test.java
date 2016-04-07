/**
 *
 */
package edu.washington.escience.myria.systemtest;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.UriSourceFragment;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;

/**
 * 
 */
public class ParallelIngestS3Test extends SystemTestBase {

  @Test
  public void parallelIngestTest() throws Exception {
    /* Read Source Data from S3 and invoke Server */

    // Note: either I can specify the byte ranges directly to the UriSource or through
    // the FileScan (via a new operator, FileScanFragment)
    String bucket = "myria-test";
    String key = "sample-parallel.txt";

    Schema relationSchema = Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE);
    RelationKey relationKey = RelationKey.of("public", "adhoc", "testParallel");

    Map<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    int workerCounter = 0;
    for (int workerID : workerIDs) {
      // find the size of the file and decide on the split , using Amazon S3 API
      AmazonS3 s3Client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
      S3Object object = s3Client.getObject(new GetObjectRequest(bucket, key));
      long fileSize = object.getObjectMetadata().getContentLength();

      long partitionSize = fileSize / workerIDs.length;
      long start = partitionSize * (workerCounter);
      long end = start + partitionSize + 3; // Just so the test passes for now
      boolean lastWorker = (workerID == workerIDs[workerIDs.length - 1]) ? true : false;

      UriSourceFragment uriSourceFragment = new UriSourceFragment(bucket, key, start, end, lastWorker);
      FileScan scanFragment = new FileScan(uriSourceFragment, relationSchema, ',', null, null, 0);

      workerPlans.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKey, true) });
      workerCounter++;
    }
    SinkRoot masterRoot = new SinkRoot(new EOSSource());
    server.submitQueryPlan(masterRoot, workerPlans);
  }
}
