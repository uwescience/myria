package edu.washington.escience.myria.daemon;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import edu.washington.escience.myria.api.MasterApiServer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule;

/**
 * This is the class for the main daemon for Myria. It manages all the various services, including
 * the API server and the Myria server.
 *
 *
 */
public final class MasterDaemon implements Task, EventHandler<CloseEvent> {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(MasterDaemon.class);

  private final CountDownLatch terminated = new CountDownLatch(1);

  /**
   * @param memento the memento object passed down by the driver.
   * @return the user defined return value
   * @throws Exception whenever the Task encounters an unsolved issue. This Exception will be thrown
   *         at the Driver's event handler.
   */
  @Override
  public byte[] call(@SuppressWarnings("unused") final byte[] memento) throws Exception {
    try {
      start();
      terminated.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      stop();
    }
    return null;
  }

  /**
   * Shut down this worker.
   */
  @Override
  public void onNext(final CloseEvent closeEvent) {
    LOGGER.info("CloseEvent received, shutting down...");
    terminated.countDown();
  }

  /** The Myria server. */
  private final Server server;
  /** The Master API server. */
  private final MasterApiServer apiServer;

  /**
   * Instantiates a MasterDaemon object. Includes the API server and the Myria server.
   *
   * @param configFilePath the dir where the config file resides.
   * @param apiPort api server port.
   * @throws Exception if there are issues loading the Catalog or instantiating the servers.
   */
  @Inject
  public MasterDaemon(
      final Server server,
      final MasterApiServer apiServer,
      final @Parameter(MyriaGlobalConfigurationModule.RestApiPort.class) int apiPort,
      final @Parameter(MyriaGlobalConfigurationModule.UseSsl.class) boolean useSsl,
      final @Parameter(MyriaGlobalConfigurationModule.SslKeystorePath.class) String keystorePath,
      final @Parameter(MyriaGlobalConfigurationModule.SslKeystorePassword.class) String
          keystorePassword,
      final @Parameter(MyriaGlobalConfigurationModule.ApiAdminPassword.class) String adminPassword)
      throws Exception {
    this.server = server;
    this.apiServer = apiServer;
    try {
      final int portMin = 1;
      final int portMax = 65535;
      if ((apiPort < portMin) || (apiPort > portMax)) {
        throw new IllegalArgumentException("port must be between " + portMin + " and " + portMax);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Start the Daemon. Namely, start the API server and the Myria Server.
   *
   * @throws Exception if there is an issue starting either server.
   */
  public void start() throws Exception {
    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);
    server.start();
    apiServer.start();
  }

  /**
   * Stop the Daemon. Namely, stop the API server and the Myria Server.
   *
   * @throws Exception if there is an issue stopping either server.
   */
  public void stop() throws Exception {
    apiServer.stop();
    server.shutdown();
  }

  /**
   * @return the cluster master
   * */
  public Server getClusterMaster() {
    return server;
  }
}
