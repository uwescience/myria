package edu.washington.escience.myria.systemtest;

import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.api.DatasetFormat;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.client.JsonQueryBaseBuilder;
import edu.washington.escience.myria.parallel.MasterQueryPartition;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

public class CollectTest extends SystemTestBase {

  @Test
  public void collectTest() throws Exception {
    HashMap<Tuple, Integer> expectedResults =
        TestUtils.tupleBatchToTupleBag(simpleSingleTableTestBase(TupleBatch.BATCH_SIZE * 2));
    JsonQueryBaseBuilder builder = new JsonQueryBaseBuilder().workers(workerIDs);

    QueryEncoding qe = builder.scan(SINGLE_TEST_TABLE).masterCollect().export(DatasetFormat.TSV).build();

    MasterQueryPartition qf = (MasterQueryPartition) server.submitQuery(qe).getQuery();
    TestUtils.assertTupleBagEqual(expectedResults, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(qf
        .getResultStream(), SINGLE_TEST_SCHEMA)));

  }
}
