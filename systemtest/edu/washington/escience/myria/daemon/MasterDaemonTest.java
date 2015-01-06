package edu.washington.escience.myria.daemon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Collections;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.mina.util.AvailablePortFinder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

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

    CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), ImmutableMap
        .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID, new SocketInfo(8001)).build(), ImmutableMap
        .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID + 1, new SocketInfo(9001)).put(
            MyriaConstants.MASTER_ID + 2, new SocketInfo(9002)).build(), Collections.<String, String> singletonMap(
        MyriaSystemConfigKeys.ADMIN_PASSWORD, "password"), Collections.<String, String> emptyMap());

    /* Remember which threads were there when the test starts. */
    Set<Thread> startThreads = ThreadUtils.getCurrentThreads();
    MasterDaemon md = new MasterDaemon(tmpFolder.getAbsolutePath(), REST_PORT);

    try {
      /* Start the master. */
      md.start();

      /* Allocate the client that we'll use to make requests. */
      Client client = ClientBuilder.newClient();

      /* First, make sure the server is up by waiting for a successful 404 response. */
      WebTarget invalidResource = client.target("http://localhost:" + REST_PORT + "/invalid");
      Invocation.Builder invalidInvocation = invalidResource.request(MediaType.TEXT_PLAIN_TYPE);
      while (true) {
        if (invalidInvocation.get().getStatus() == Status.NOT_FOUND.getStatusCode()) {
          break;
        }
      }

      /* Try to stop the master with no auth header. */
      Invocation.Builder shutdownInvocation =
          client.target("http://localhost:" + REST_PORT + "/server/shutdown").request(MediaType.TEXT_PLAIN_TYPE);
      assertEquals(Status.UNAUTHORIZED.getStatusCode(), shutdownInvocation.get().getStatus());

      client.register(HttpAuthenticationFeature.basicBuilder().build());
      shutdownInvocation =
          client.target("http://localhost:" + REST_PORT + "/server/shutdown").request(MediaType.TEXT_PLAIN_TYPE);
      /* Try to stop the master with a non-admin username. */
      shutdownInvocation.property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_USERNAME, "jwang").property(
          HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_PASSWORD, "somepassword");
      assertEquals(Status.FORBIDDEN.getStatusCode(), shutdownInvocation.get().getStatus());

      /* Try to stop the master with a wrong admin password. */
      shutdownInvocation.property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_USERNAME, "admin").property(
          HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_PASSWORD, "wrongpassword");
      assertEquals(Status.UNAUTHORIZED.getStatusCode(), shutdownInvocation.get().getStatus());

      /* Provide the correct admin password and stop the master. */
      shutdownInvocation.property(HttpAuthenticationFeature.HTTP_AUTHENTICATION_BASIC_PASSWORD, "password");
      assertEquals(Status.OK.getStatusCode(), shutdownInvocation.get().getStatus());
      client.close();

    } catch (Throwable e) {
      /* Stop the master. */
      md.stop();
      throw e;
    } finally {
      /* Wait for all threads that weren't there when we started to finish. */
      Set<Thread> doneThreads = ThreadUtils.getCurrentThreads();
      for (Thread t : doneThreads) {
        if (!t.isDaemon() && !startThreads.contains(t)) {
          t.join();
        }
      }

      FSUtils.deleteFileFolder(tmpFolder);
      while (!AvailablePortFinder.available(8001) || !AvailablePortFinder.available(REST_PORT)) {
        Thread.sleep(100);
      }
    }
  }

  @Test(timeout = TIMEOUT_MS)
  public void testNoAdminPasswordCfgFile() throws Exception {
    File tmpFolder = Files.createTempDir();
    CatalogMaker.makeNNodesLocalParallelCatalog(tmpFolder.getAbsolutePath(), ImmutableMap
        .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID, new SocketInfo(8001)).build(), ImmutableMap
        .<Integer, SocketInfo> builder().put(MyriaConstants.MASTER_ID + 1, new SocketInfo(9001)).put(
            MyriaConstants.MASTER_ID + 2, new SocketInfo(9002)).build(), Collections.<String, String> emptyMap(),
        Collections.<String, String> emptyMap());

    MasterDaemon md = new MasterDaemon(tmpFolder.getAbsolutePath(), REST_PORT);
    md.start();
    Client client = ClientBuilder.newClient();
    Invocation.Builder invocation =
        client.target("http://localhost:" + REST_PORT + "/server/deployment_cfg").request(MediaType.TEXT_PLAIN_TYPE);
    Response response = invocation.get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String deploymentFile = md.getClusterMaster().getConfiguration(MyriaSystemConfigKeys.DEPLOYMENT_FILE);
    String respond = response.readEntity(String.class);
    /*
     * Tests and cluster deployments have different working directory hierarchies (no description-files). So here we
     * only compare the suffix. TODO: unify these two.
     */
    assertTrue(respond.endsWith(deploymentFile));
    client.close();
    md.stop();
    FSUtils.deleteFileFolder(tmpFolder);
  }
}
