package edu.washington.escience.myria.daemon;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collections;
import java.util.Set;

import javax.ws.rs.core.Response.Status;

import org.apache.mina.util.AvailablePortFinder;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.coordinator.catalog.CatalogMaker;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.util.FSUtils;
import edu.washington.escience.myria.util.ThreadUtils;

public class MasterDaemonTest {
  /* Test should timeout after 20 seconds. */
  public static final int TIMEOUT_MS = 20 * 1000;
  /* Port used for the master daemon */
  public static final int REST_PORT = 8753;

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndShutdown() throws Exception {
    File tmpFolder = Files.createTempDir();
    try {
      CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), ImmutableMap
          .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID, new SocketInfo(8001)).build(), ImmutableMap
          .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID + 1, new SocketInfo(9001)).put(
              MyriaConstants.MASTER_ID + 2, new SocketInfo(9002)).build(), Collections.<String, String> emptyMap(),
          Collections.<String, String> emptyMap());

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
      CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), ImmutableMap
          .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID, new SocketInfo(8001)).build(), ImmutableMap
          .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID + 1, new SocketInfo(9001)).put(
              MyriaConstants.MASTER_ID + 2, new SocketInfo(9002)).build(), Collections.<String, String> singletonMap(
          MyriaSystemConfigKeys.ADMIN_PASSWORD, "password"), Collections.<String, String> emptyMap());

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

      /* Try to stop the master without saying I'm admin. */
      WebResource shutdownRest = client.resource("http://localhost:" + REST_PORT + "/server/shutdown");
      ClientResponse response = shutdownRest.get(ClientResponse.class);
      assertEquals(Status.FORBIDDEN.getStatusCode(), response.getStatus());

      /* Try to stop the master with a wrong admin password. */
      client.addFilter(new HTTPBasicAuthFilter("admin", "wrongpassword"));
      response = shutdownRest.get(ClientResponse.class);
      assertEquals(Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
      client.removeAllFilters();

      /* Provide the password and stop the master. */
      client.addFilter(new HTTPBasicAuthFilter("admin", "password"));
      response = shutdownRest.get(ClientResponse.class);
      assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
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
