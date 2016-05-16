/**
 *
 */
package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 *
 */
public class CatalogScanTest {

  /**
   * The catalog
   */
  private MasterCatalog catalog;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    /* Turn off SQLite logging, it's annoying. */
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);

    catalog = MasterCatalog.createInMemory();

    // add a query to the catalog
    QueryEncoding query = new QueryEncoding();
    query.rawQuery = "query 1 is about baseball";
    query.logicalRa = "";
    catalog.newQuery(query);
  }

  /**
   * Destroy catalog.
   */
  @After
  public void Cleanup() {
    catalog.close();
  }

  @Test
  public final void testQueryQueries() throws DbException, CatalogException {
    Schema schema = Schema.ofFields(Type.LONG_TYPE, "id", Type.STRING_TYPE, "raw");
    CatalogQueryScan scan =
        new CatalogQueryScan("select query_id, raw_query from queries", schema, catalog);
    scan.open(null);

    assertEquals(false, scan.eos());
    TupleBatch tb = scan.nextReady();
    assertEquals(false, scan.eos());
    assertEquals(1, tb.numTuples());
    assertEquals(schema, tb.getSchema());

    tb = scan.nextReady();
    assertEquals(true, scan.eos());
  }
}
