package edu.washington.escience.myria.daemon;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.task.Task;

import edu.washington.escience.myria.api.MasterApiServer;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.ApiAdminPassword;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.SslKeystorePassword;
import edu.washington.escience.myria.tools.MyriaGlobalConfigurationModule.SslKeystorePath;

/**
 * This is the class for the main daemon for Myria. It manages all the various services, including
 * the API server and the Myria server.
 * 
 * 
 */
public final class MasterDaemon implements Task {

  /**
   * @param memento the memento object passed down by the driver.
   * @return the user defined return value
   * @throws Exception whenever the Task encounters an unresolved issue. This Exception will be
   *         thrown at the Driver's event handler.
   */
  @Override
  public byte[] call(@SuppressWarnings("unused") final byte[] memento) throws Exception {
    start();
    return null;
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
      final @Parameter(MyriaGlobalConfigurationModule.RestApiPort.class) int apiPort,
      final @Parameter(MyriaDriverLauncher.SerializedGlobalConf.class) String serializedGlobalConf)
      throws Exception {
    Injector globalConfInjector;
    try {
      Configuration globalConf = new AvroConfigurationSerializer().fromString(serializedGlobalConf);
      globalConfInjector = Tang.Factory.getTang().newInjector(globalConf);
      final int portMin = 1;
      final int portMax = 65535;
      if ((apiPort < portMin) || (apiPort > portMax)) {
        throw new IllegalArgumentException("port must be between " + portMin + " and " + portMax);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }
    server = new Server(globalConfInjector);
    try {
      String keystorePath = globalConfInjector.getNamedInstance(SslKeystorePath.class);
      String keystorePassword = globalConfInjector.getNamedInstance(SslKeystorePassword.class);
      String adminPassword = globalConfInjector.getNamedInstance(ApiAdminPassword.class);
      apiServer =
          new MasterApiServer(server, this, apiPort, keystorePath, keystorePassword, adminPassword);
    } catch (Exception e) {
      server.shutdown();
      throw e;
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
