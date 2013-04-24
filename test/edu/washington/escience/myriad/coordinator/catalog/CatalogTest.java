package edu.washington.escience.myriad.coordinator.catalog;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.parallel.SocketInfo;

public class CatalogTest {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CatalogTest.class);

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      LOGGER.warn("*********************************************");
      LOGGER.warn(String.format("Starting test: %s()...", description.getMethodName()));
      LOGGER.warn("*********************************************");
    };
  };

  /**
   * TODO All this test does is create an in-memory Catalog and make sure its description matches. Flesh it out.
   * 
   * @throws CatalogException if there is an error creating the Catalog.
   */
  @Test
  public void testCatalogCreation() throws CatalogException {
    /* Constants needed for this test. */
    final String DESCRIPTION = "local-worker-test";
    final String SERVER = "localhost:8001";
    final String[] WORKERS = { "localhost:9001", "localhost:9002" };

    /* Turn off SQLite logging, it's annoying. */
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);

    Catalog catalog = null;

    /* Set and test the description */
    catalog = Catalog.createInMemory(DESCRIPTION);
    assertTrue(catalog.getDescription().equals(DESCRIPTION));

    /* Set and test the server */
    catalog.addMaster(SERVER);
    final List<SocketInfo> servers = catalog.getMasters();
    assertTrue(servers.size() == 1);
    assertTrue(servers.get(0).toString().equals(SERVER));

    /* Set and test the workers */
    for (final String worker : WORKERS) {
      catalog.addWorker(worker);
    }
    final Map<Integer, SocketInfo> workers = catalog.getWorkers();
    assertTrue(workers.size() == WORKERS.length);
    /* Slightly annoying equality check here */
    final Collection<SocketInfo> values = workers.values();
    for (final String worker : WORKERS) {
      assertTrue(values.contains(SocketInfo.valueOf(worker)));
    }
  }

  /**
   * Test creating a catalog using CatalogMaker functionality.
   * 
   * @throws Exception if there is an error making the Catalog.
   */
  @Test
  public void testCatalogMaker() throws Exception {
    /* Turn off SQLite logging, it's annoying. */
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);
    Path path = Files.createTempDirectory(MyriaConstants.SYSTEM_NAME + "_CatalogTest");
    final int numWorkers = 5;
    final String masterCatalogPath = path.toString() + File.separatorChar + "master.catalog";
    CatalogMaker.makeNNodesLocalParallelCatalog(path.toFile().getAbsolutePath(), 5);
    Catalog c = Catalog.open(masterCatalogPath);
    assertTrue(c.getWorkers().size() == numWorkers);
    c.close();
  }
}
