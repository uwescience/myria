/**
 *
 */
package edu.washington.escience.myria.systemtest;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.NChiladaFileScan;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;

public class NChiladaTest extends SystemTestBase {

  @Test
  public void ingestTest() throws Exception {

    RelationKey relationKey = RelationKey.of("public", "adhoc", "testNChilada");
    Map<Integer, RootOperator[]> workerPlans = new HashMap<Integer, RootOperator[]>();

    for (int workerID : workerIDs) {
      // Kill this and don't let it run
      NChiladaFileScan scanFragment = new NChiladaFileScan(
          "https://s3-us-west-2.amazonaws.com/uwdb/cosmoVulcan/cosmo25p.768sg1bwK1C52.001818",
          "https://s3-us-west-2.amazonaws.com/uwdb/cosmoVulcan/cosmo25p.768sg1bwK1C52.001818.rockstar.grp");

      workerPlans.put(workerID, new RootOperator[] { new DbInsert(scanFragment, relationKey, true) });
    }
    SinkRoot masterRoot = new SinkRoot(new EOSSource());
    // QueryFuture qf = server.submitQueryPlan(masterRoot, workerPlans);
    // assertEquals(Status.SUCCESS, server.getQueryManager().getQueryStatus(qf.getQueryId()).status);
  }
}