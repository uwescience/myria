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
  private static final String USAGE_STRING = "Usage: MasterDaemon <catalogFilename>";

  /**
   * @param args the command-line arguments to start the daemon.
   * @throws Exception if the Restlet server can't start.
   */
  public static void main(final String[] args) throws Exception {
    final MasterDaemon md = new MasterDaemon(args);
    md.start();
    // System.out.println("Press enter to stop");
    // System.in.read();
    // md.stop();
  }

  /** The Myriad server. */
  private final Server server;

  /**
   * Instantiates a MasterDaemon object. Includes the API server and the Myriad server.
   * 
   * @param args the command-line arguments. Right now, args[0] must be the Catalog file.
   * @throws Exception if there are issues loading the Catalog or instantiating the servers.
   */
  public MasterDaemon(final String[] args) throws Exception {
    processArguments(args);
    server = new Server(FilenameUtils.concat(args[0], "master.catalog"));
    MasterApiServer.setUp(server);
  }

  /**
   * @param args the command-line arguments to start the daemon.
   * @throws CatalogException if the Catalog cannot be opened.
   * @throws FileNotFoundException if the catalogFile does not exist.
   */
  private void processArguments(final String[] args) throws FileNotFoundException, CatalogException {
    if (args.length != 1) {
      throw new IllegalArgumentException(USAGE_STRING);
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
    MasterApiServer.INSTANCE.start();
    server.start();
  }

  /**
   * Stop the Daemon. Namely, stop the API server and the Myriad Server.
   * 
   * @throws Exception if there is an issue stopping either server.
   */
  public void stop() throws Exception {
    MasterApiServer.INSTANCE.stop();
    server.shutdown();
  }

}
