/**
 *
 */
package edu.washington.escience.myria.systemtest;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.washington.escience.myria.CsvTupleReader;
import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.DataSink;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.io.UriSink;
import edu.washington.escience.myria.io.UriSource;
import edu.washington.escience.myria.operator.DataInput;
import edu.washington.escience.myria.operator.DataOutput;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.DbQueryScan;
import edu.washington.escience.myria.operator.InMemoryOrderBy;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.network.CollectConsumer;
import edu.washington.escience.myria.operator.network.CollectProducer;
import edu.washington.escience.myria.operator.network.GenericShuffleConsumer;
import edu.washington.escience.myria.operator.network.GenericShuffleProducer;
import edu.washington.escience.myria.operator.network.partition.RoundRobinPartitionFunction;
import edu.washington.escience.myria.operator.network.partition.SingleFieldHashPartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.JsonAPIUtils;

/**
 */
public class UploadDownloadS3Test extends SystemTestBase {

  @Test
  public void s3UploadTest() throws Exception {

    /* Ingest test data */
    String filePath =
        Paths.get("testdata", "filescan", "simple_two_col_int_to_hash.txt").toString();
    DataSource relationSource = new FileSource(filePath);
    RelationKey relationKeyUpload = RelationKey.of("public", "adhoc", "upload");
    Schema relationSchema = Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE);

    JsonAPIUtils.ingestData(
        "localhost",
        masterDaemonPort,
        ingest(
            relationKeyUpload,
            relationSchema,
            relationSource,
            ' ',
            new RoundRobinPartitionFunction(workerIDs.length)));

    /* File to upload and download */
    String fileName = String.format("s3://myria-test/test-%d.txt", System.currentTimeMillis());

    /* Construct the query and upload data */
    ExchangePairID serverReceiveID = ExchangePairID.newID();
    DbQueryScan workerScan = new DbQueryScan(relationKeyUpload, relationSchema);
    CollectProducer workerProducer = new CollectProducer(workerScan, serverReceiveID, MASTER_ID);

    Map<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlans.put(workerID, new RootOperator[] {workerProducer});
    }
    CollectConsumer serverConsumer =
        new CollectConsumer(relationSchema, serverReceiveID, workerIDs);
    InMemoryOrderBy sortOperator =
        new InMemoryOrderBy(serverConsumer, new int[] {1}, new boolean[] {true});
    DataSink dataSink = new UriSink(fileName);
    DataOutput masterRoot = new DataOutput(sortOperator, new CsvTupleWriter(), dataSink);
    server.submitQueryPlan(masterRoot, workerPlans).get();

    /* Read the data back in from S3 and shuffle to one worker */
    RelationKey relationKeyDownload = RelationKey.of("public", "adhoc", "download");
    DataSource relationSourceS3 = new UriSource(fileName);

    ExchangePairID workerReceiveID = ExchangePairID.newID();
    DataInput serverInput = new DataInput(new CsvTupleReader(relationSchema, ',', null, null, 1), relationSourceS3);
    GenericShuffleProducer serverProduce =
        new GenericShuffleProducer(serverInput, workerReceiveID, workerIDs, new SingleFieldHashPartitionFunction(
            workerIDs.length, 0));
    GenericShuffleConsumer workerConsumer =
        new GenericShuffleConsumer(relationSchema, workerReceiveID, new int[] {MASTER_ID});
    DbInsert workerInsert = new DbInsert(workerConsumer, relationKeyDownload, true);
    Map<Integer, RootOperator[]> workerPlansInsert = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlansInsert.put(workerID, new RootOperator[] {workerInsert});
    }
    server.submitQueryPlan(serverProduce, workerPlansInsert).get();

    String dstData =
        JsonAPIUtils.download(
            "localhost",
            masterDaemonPort,
            relationKeyDownload.getUserName(),
            relationKeyDownload.getProgramName(),
            relationKeyDownload.getRelationName(),
            "json");

    String srcData =
        "[{\"x\":1,\"y\":2},{\"x\":1,\"y\":2},{\"x\":1,\"y\":4},{\"x\":1,\"y\":4},{\"x\":1,\"y\":6},{\"x\":1,\"y\":6}]";

    assertEquals(srcData, dstData);
  }
}
