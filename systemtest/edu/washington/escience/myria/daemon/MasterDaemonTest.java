package edu.washington.escience.myria.daemon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.io.FilenameUtils;
import org.apache.mina.util.AvailablePortFinder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.coordinator.ConfigFileGenerator;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.tool.MyriaConfiguration;
import edu.washington.escience.myria.util.DeploymentUtils;
import edu.washington.escience.myria.util.FSUtils;
import edu.washington.escience.myria.util.ThreadUtils;

public class MasterDaemonTest {
  /* Test should timeout after 20 seconds. */
  public static final int TIMEOUT_MS = 20 * 1000;
  /* Port used for the master daemon */
  public static final int REST_PORT = 8753;
  /* temp folder to put master configuration. */
  private String testBaseFolder;
  /* Description. */
  private static final String DESCRIPTION = "masterDaemonTest";
  /* working directory. */
  private String workingDir;

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndShutdown() throws Exception {
    try {
      /* Remember which threads were there when the test starts. */
      Set<Thread> startThreads = ThreadUtils.getCurrentThreads();

      /* Start the master. */
      MasterDaemon md = new MasterDaemon(workingDir, REST_PORT);
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
      while (!AvailablePortFinder.available(8001) || !AvailablePortFinder.available(REST_PORT)) {
        Thread.sleep(100);
      }
    }
  }

  @Test(timeout = TIMEOUT_MS)
  public void testStartAndRestShutdown() throws Exception {

    /* Remember which threads were there when the test starts. */
    Set<Thread> startThreads = ThreadUtils.getCurrentThreads();
    MasterDaemon md = new MasterDaemon(workingDir, REST_PORT);

    md.getClusterMaster().getConfig().setValue("deployment", MyriaSystemConfigKeys.ADMIN_PASSWORD, "password");

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
      while (!AvailablePortFinder.available(8001) || !AvailablePortFinder.available(REST_PORT)) {
        Thread.sleep(100);
      }
    }
  }

  @Test(timeout = TIMEOUT_MS)
  public void testNoAdminPasswordCfgFile() throws Exception {
    MasterDaemon md = new MasterDaemon(workingDir, REST_PORT);
    md.start();
    Client client = ClientBuilder.newClient();
    Invocation.Builder invocation =
        client.target("http://localhost:" + REST_PORT + "/server/deployment_cfg").request(MediaType.TEXT_PLAIN_TYPE);
    Response response = invocation.get();
    assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String respond = response.readEntity(String.class);
    assertTrue(respond.equals(Paths.get(workingDir, MyriaConstants.DEPLOYMENT_CONF_FILE).toString()));
    client.close();
    md.stop();
  }

  @Before
  public void initialize() throws Exception {
    testBaseFolder = Files.createTempDirectory("masterDaemonTest").toString();
    workingDir = FilenameUtils.concat(testBaseFolder, DESCRIPTION);
    MasterCatalog.create(DeploymentUtils.getPathToMasterDir(workingDir));
    final String configFile = generateTestConfFile();
    ConfigFileGenerator.makeWorkerConfigFiles(configFile, workingDir);
  }

  @After
  public void cleanup() throws Exception {
    FSUtils.blockingDeleteDirectory(testBaseFolder);
  }

  /**
   * 
   * @return the path to the config file
   * @throws IOException IOException
   */
  private String generateTestConfFile() throws IOException {
    MyriaConfiguration config = MyriaConfiguration.newConfiguration();
    config.setValue("deployment", MyriaSystemConfigKeys.DEPLOYMENT_PATH, testBaseFolder);
    config.setValue("deployment", MyriaSystemConfigKeys.DESCRIPTION, DESCRIPTION);
    config.setValue("deployment", MyriaSystemConfigKeys.WORKER_STORAGE_DATABASE_SYSTEM,
        MyriaConstants.STORAGE_SYSTEM_SQLITE);
    config.setValue("master", MyriaConstants.MASTER_ID + "", "localhost:8001");
    config.setValue("workers", (MyriaConstants.MASTER_ID + 1) + "", "localhost:9001");
    config.setValue("workers", (MyriaConstants.MASTER_ID + 2) + "", "localhost:9002");
    Files.createDirectories(Paths.get(workingDir));
    File configFile = Paths.get(workingDir, MyriaConstants.DEPLOYMENT_CONF_FILE).toFile();
    config.write(configFile);
    return configFile.getAbsolutePath();
  }
}
