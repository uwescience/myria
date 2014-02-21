package edu.washington.escience.myria.systemtest;

import java.util.HashMap;

import org.junit.Test;

import edu.washington.escience.myria.api.DatasetFormat;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.client.JsonQueryBaseBuilder;
import edu.washington.escience.myria.parallel.MasterQueryPartition;
import edu.washington.escience.myria.util.TestUtils;
import edu.washington.escience.myria.util.Tuple;

/**
 * 
 * Test Broadcast Operator
 * 
 * This test performs a broadcast join. There are two relations, testtable1 and testtable2, distributed among workers.
 * This test program broadcast testtable1 and then join it locally with testtable2. After that, collect operator is used
 * to collect result.
 * 
 * 
 * @author Shumo Chu (chushumo@cs.washington.edu)
 * 
 */
public class BroadcastTest extends SystemTestBase {

  @Test
  public void broadcastTest() throws Exception {

    /* use some tables generated in simpleRandomJoinTestBase */
    final HashMap<Tuple, Integer> expectedResult = simpleRandomJoinTestBase();

    JsonQueryBaseBuilder builder = new JsonQueryBaseBuilder().workers(workerIDs);

    QueryEncoding query = builder.scan(JOIN_TEST_TABLE_2)//
        .hashEquiJoin(//
            builder.scan(JOIN_TEST_TABLE_1).broadcast()//
            , new int[] { 0 }, new int[] { 0, 1 }, new int[] { 0 }, new int[] { 0, 1 })//
        .masterCollect()//
        .export(DatasetFormat.TSV)//
        .build();

    MasterQueryPartition qf = (MasterQueryPartition) server.submitQuery(query).getQuery();
    TestUtils.assertTupleBagEqual(expectedResult, TestUtils.tupleBatchToTupleBag(TestUtils.parseTSV(qf
        .getResultStream(), qf.getResultSchema())));
  }

}
