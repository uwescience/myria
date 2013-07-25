package edu.washington.escience.myriad.daemon;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import org.apache.mina.util.AvailablePortFinder;
import org.junit.Test;

import com.google.common.io.Files;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import edu.washington.escience.myriad.MyriaSystemConfigKeys;
import edu.washington.escience.myriad.coordinator.catalog.CatalogMaker;
import edu.washington.escience.myriad.util.FSUtils;
import edu.washington.escience.myriad.util.ThreadUtils;

public class MasterDaemonTest {
  /* Test should timeout after 20 seconds. */
  public static final int TIMEOUT_MS = 20 * 1000;
  /* Port used for the master daemon */
  public static final int REST_PORT = 8753;

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
      MasterDaemon md = new MasterDaemon(tmpFolder.getAbsolutePath(), REST_PORT);
      md.start();

      /* Stop the master. */
      md.stop();

      /* Wait for all threads that weren't there when we started to finish. */
      Set<Thread> doneThreads = ThreadUtils.getCurrentThreads();
      for (Thread t : doneThreads) {
        if (!t.isDaemon() && !startThreads.contains(t)) {
          t.join();
        }
      }
    } finally {
      FSUtils.deleteFileFolder(tmpFolder);
      while (!AvailablePortFinder.available(8001) || !AvailablePortFinder.available(REST_PORT)) {
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
      MasterDaemon md = new MasterDaemon(tmpFolder.getAbsolutePath(), REST_PORT);
      md.start();

      /* Allocate the client that we'll use to make requests. */
      Client client = Client.create();

      /* First, make sure the server is up by waiting for a successful 404 response. */
      WebResource invalidResource = client.resource("http://localhost:" + REST_PORT + "/invalid");
      while (true) {
        ClientResponse response = invalidResource.get(ClientResponse.class);
        if (response.getStatus() == Status.NOT_FOUND.getStatusCode()) {
          break;
        }
      }

      /* Stop the master. */
      WebResource shutdownRest = client.resource("http://localhost:" + REST_PORT + "/server/shutdown");
      ClientResponse response = shutdownRest.get(ClientResponse.class);
      assertTrue(response.getStatus() == Status.NO_CONTENT.getStatusCode());
      client.destroy();

      /* Wait for all threads that weren't there when we started to finish. */
      Set<Thread> doneThreads = ThreadUtils.getCurrentThreads();
      for (Thread t : doneThreads) {
        if (!t.isDaemon() && !startThreads.contains(t)) {
          t.join();
        }
      }
    } finally {
      FSUtils.deleteFileFolder(tmpFolder);
      while (!AvailablePortFinder.available(8001) || !AvailablePortFinder.available(REST_PORT)) {
        Thread.sleep(100);
      }
    }
  }
}
