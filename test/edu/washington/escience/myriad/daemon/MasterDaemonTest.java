package edu.washington.escience.myriad.daemon;

import java.io.File;
import java.util.Set;

import org.apache.mina.util.AvailablePortFinder;
import org.junit.Test;
import org.restlet.resource.ClientResource;

import com.google.common.io.Files;

import edu.washington.escience.myriad.api.MasterApiServer;
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
      final int baseMasterPort = 8001;
      final int baseWorkerPort = 9001;
      final int n = 0;
      final String[] args = new String[n + 3];
      args[0] = tmpFolder.getAbsolutePath();
      args[1] = Integer.toString(n);
      args[2] = "localhost:" + baseMasterPort;
      for (int i = 0; i < n; ++i) {
        args[i + 3] = "localhost:" + (baseWorkerPort + i);
      }
      CatalogMaker.makeNNodesParallelCatalog(args);

      /* Remember which threads were there when the test starts. */
      Set<Thread> startThreads = ThreadUtils.getCurrentThreads();

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
      while (!AvailablePortFinder.available(8001) || !AvailablePortFinder.available(MasterApiServer.PORT)) {
        Thread.sleep(100);
      }
    }
  }

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndRestShutdown() throws Exception {

    File tmpFolder = Files.createTempDir();
    try {
      final int baseMasterPort = 8001;
      final int baseWorkerPort = 9001;
      final int n = 0;
      final String[] args = new String[n + 3];
      args[0] = tmpFolder.getAbsolutePath();
      args[1] = Integer.toString(n);
      args[2] = "localhost:" + baseMasterPort;
      for (int i = 0; i < n; ++i) {
        args[i + 3] = "localhost:" + (baseWorkerPort + i);
      }
      CatalogMaker.makeNNodesParallelCatalog(args);

      /* Remember which threads were there when the test starts. */
      Set<Thread> startThreads = ThreadUtils.getCurrentThreads();

      /* Start the master. */
      MasterDaemon md = new MasterDaemon(new String[] { tmpFolder.getAbsolutePath() });
      md.start();

      /* Stop the master. */
      ClientResource shutdownRest = new ClientResource("http://localhost:" + MasterApiServer.PORT + "/server/shutdown");
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
      while (!AvailablePortFinder.available(8001) || !AvailablePortFinder.available(MasterApiServer.PORT)) {
        Thread.sleep(100);
      }
    }
  }

}
