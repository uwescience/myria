package edu.washington.escience.myriad.daemon;

import java.io.FileNotFoundException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FilenameUtils;

import edu.washington.escience.myriad.api.MasterApiServer;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.parallel.Server;

/**
 * This is the class for the main daemon for Myriad. It manages all the various services, including the API server and
 * the Myriad server.
 * 
 * @author dhalperi
 * 
 */
public final class MasterDaemon {

  /** The usage string. */
  private static final String USAGE_STRING = "Usage: MasterDaemon <catalogFilename> <restPort>";

  /**
   * @param args the command-line arguments to start the daemon.
   * @throws Exception if the Restlet server can't start.
   */
  public static void main(final String[] args) throws Exception {
    processArguments(args);
    String catalogDirPath = args[0];
    int apiPort = Integer.parseInt(args[1]);
    final MasterDaemon md = new MasterDaemon(catalogDirPath, apiPort);
    md.start();
  }

  /** The Myria server. */
  private final Server server;
  /** The Master API server. */
  private final MasterApiServer apiServer;

  /**
   * Instantiates a MasterDaemon object. Includes the API server and the Myriad server.
   * 
   * @param catalogDirPath the dir where the Catalog file resides.
   * @param apiPort api server port.
   * @throws Exception if there are issues loading the Catalog or instantiating the servers.
   */
  public MasterDaemon(final String catalogDirPath, final int apiPort) throws Exception {
    server = new Server(FilenameUtils.concat(catalogDirPath, "master.catalog"));
    apiServer = new MasterApiServer(server, this, apiPort);
  }

  /**
   * @param args the command-line arguments to start the daemon.
   * @throws CatalogException if the Catalog cannot be opened.
   * @throws FileNotFoundException if the catalogFile does not exist.
   */
  private static void processArguments(final String[] args) throws FileNotFoundException, CatalogException {
    /* Check length. */
    if (args.length != 2) {
      throw new IllegalArgumentException(USAGE_STRING);
    }

    /* Check port. */
    try {
      int port = Integer.parseInt(args[1]);
      final int portMin = 1;
      final int portMax = 65535;
      if ((port < portMin) || (port > portMax)) {
        throw new IllegalArgumentException("port must be between " + portMin + " and " + portMax);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(e);
    }

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);
  }

  /**
   * Start the Daemon. Namely, start the API server and the Myriad Server.
   * 
   * @throws Exception if there is an issue starting either server.
   */
  public void start() throws Exception {
    server.start();
    apiServer.start();
  }

  /**
   * Stop the Daemon. Namely, stop the API server and the Myriad Server.
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
