package edu.washington.escience.myriad.daemon;

import java.io.File;
import java.util.HashMap;
import java.util.Set;

import org.apache.mina.util.AvailablePortFinder;
import org.junit.Test;
import org.restlet.resource.ClientResource;

import com.google.common.io.Files;

import edu.washington.escience.myriad.MyriaSystemConfigKeys;
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
      HashMap<String, String> mc = new HashMap<String, String>();
      mc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "8001");
      HashMap<String, String> wc = new HashMap<String, String>();
      wc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "9001");
      CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), 2, mc, wc);

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
      HashMap<String, String> mc = new HashMap<String, String>();
      mc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "8001");
      HashMap<String, String> wc = new HashMap<String, String>();
      wc.put(MyriaSystemConfigKeys.IPC_SERVER_PORT, "9001");
      CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), 2, mc, wc);

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
