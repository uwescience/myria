/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.io.UriSink;
import edu.washington.escience.myria.io.UriSource;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.FileScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.JsonAPIUtils;
import edu.washington.escience.myria.util.TestEnvVars;

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

    /* File to upload and download */
    String fileName = String.format("s3://myria-test/test.txt");

    /* Construct the query and upload data */
    ExchangePairID serverReceiveID = ExchangePairID.newID();
    DbQueryScan dbScan = new DbQueryScan(relationKey, relationSchema);
    CollectProducer dbCollect = new CollectProducer(dbScan, serverReceiveID, MASTER_ID);
    HashMap<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlans.put(workerID, new RootOperator[] { dbCollect });
    }
    CollectConsumer serverCollect = new CollectConsumer(relationSchema, serverReceiveID, workerIDs);
    DataSink dataSink = new UriSink(fileName);
    DataOutput masterRoot = new DataOutput(serverCollect, new CsvTupleWriter(), dataSink);
    server.submitQueryPlan(masterRoot, workerPlans).get();

    /* Read the data back in from S3 for each chunk and verify */
    int totalTupleCount = 0;
    DataSource relationSourceS3 = new UriSource(fileName);
    FileScan scan = new FileScan(relationSource, relationSchema, ' ');

    scan.open(TestEnvVars.get());
    while (!scan.eos()) {
      TupleBatch tb = scan.nextReady();
      if (tb != null) {
        totalTupleCount += tb.numTuples();
      }
    }
    scan.close();

    int expectedTupleCount = 7;
    assertEquals(totalTupleCount, expectedTupleCount);
  }
}
