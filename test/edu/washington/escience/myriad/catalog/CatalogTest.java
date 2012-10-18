package edu.washington.escience.myriad.catalog;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

public class CatalogTest {

  /**
   * TODO All this test does is create an in-memory Catalog and make sure its description matches. Flesh it out.
   */
  @Test
  public void testCatalogCreation() {
    /* Constants needed for this test. */
    final String DESCRIPTION = "local-worker-test";

    /* Turn off SQLite logging, it's annoying. */
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);

    try {
      Catalog catalog = Catalog.createInMemory(DESCRIPTION);
      assertTrue(catalog.getDescription().equals(DESCRIPTION));
    } catch (CatalogException e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }
}
