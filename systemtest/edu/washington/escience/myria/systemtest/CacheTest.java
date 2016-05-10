/**
 *
 */
package edu.washington.escience.myria.systemtest;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.io.UriSource;
import edu.washington.escience.myria.operator.CSVFileScanFragment;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;

/**
 * 
 */
public class CacheTest extends SystemTestBase {

  @Test
  public void test() throws URISyntaxException, InterruptedException, ExecutionException, DbException, CatalogException {
    RelationKey relationKey = RelationKey.of("public", "adhoc", "upload");
    Schema relationSchema = Schema.ofFields("x", Type.INT_TYPE, "y", Type.INT_TYPE);
    CSVFileScanFragment scan =
        new CSVFileScanFragment(new UriSource("s3://tpchssb/lineorderOUT.csv"), relationSchema, 2);
    DbInsert insert = new DbInsert(scan, relationKey, true);
    Map<Integer, RootOperator[]> workerPlansInsert = new HashMap<Integer, RootOperator[]>();
    for (int workerID : workerIDs) {
      workerPlansInsert.put(workerID, new RootOperator[] { insert });
    }
    server.submitQueryPlan(new SinkRoot(new EOSSource()), workerPlansInsert).get();
  }

}
