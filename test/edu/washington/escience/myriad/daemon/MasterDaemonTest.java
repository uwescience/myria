package edu.washington.escience.myriad.daemon;

import java.io.File;
import java.util.Set;

import org.junit.Test;
import org.restlet.resource.ClientResource;

import com.google.common.io.Files;

import edu.washington.escience.myriad.coordinator.catalog.CatalogMaker;
import edu.washington.escience.myriad.util.FSUtils;
import edu.washington.escience.myriad.util.ThreadUtils;

public class MasterDaemonTest {
  /* Test should timeout after 20 seconds. */
  public static final int TIMEOUT_MS = 20 * 1000;

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndShutdown() throws Exception {
    File tmpFolder = Files.createTempDir();
    try {
      /* Remember which threads were there when the test starts. */
      Set<Thread> startThreads = ThreadUtils.getCurrentThreads();

      CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), 2);

      /* Start the master. */
      MasterDaemon md = new MasterDaemon(new String[] { tmpFolder.getAbsolutePath() });
      md.start();

      /* Stop the master. */
      md.stop();

      /* Wait for all threads that weren't there when we started to finish. */
      Set<Thread> doneThreads = ThreadUtils.getCurrentThreads();
      for (Thread t : doneThreads) {
        if (!startThreads.contains(t)) {
          t.join();
        }
      }
    } finally {
      FSUtils.deleteFileFolder(tmpFolder);
    }
  }

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndRestShutdown() throws Exception {

    File tmpFolder = Files.createTempDir();

    try {
      /* Remember which threads were there when the test starts. */
      Set<Thread> startThreads = ThreadUtils.getCurrentThreads();

      CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), 2);

      /* Start the master. */
      MasterDaemon md = new MasterDaemon(new String[] { tmpFolder.getAbsolutePath() });
      md.start();

      /* Stop the master. */
      ClientResource shutdownRest = new ClientResource("http://localhost:8753/server/shutdown");
      shutdownRest.get();
      shutdownRest.release();

      /* Wait for all threads that weren't there when we started to finish. */
      Set<Thread> doneThreads = ThreadUtils.getCurrentThreads();
      for (Thread t : doneThreads) {
        if (!startThreads.contains(t)) {
          t.join();
        }
      }
    } finally {
      FSUtils.deleteFileFolder(tmpFolder);
    }
  }

}
