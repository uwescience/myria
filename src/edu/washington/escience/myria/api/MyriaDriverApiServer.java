package edu.washington.escience.myria.api;

import java.io.IOException;
import java.net.URI;

import javax.inject.Inject;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.daemon.MyriaDriver;

public class MyriaDriverApiServer {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterApiServer.class);

  /** The Jersey web server. */
  private final HttpServer webServer;

  //Where is this declared?
  @Inject
  public MyriaDriverApiServer(final MyriaDriver driver) throws IOException {
    URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(8777).build();
    ResourceConfig masterApplication = new DriverApplication(driver);
    LOGGER.info("Not enabling SSL");
    webServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, masterApplication);
  }

  /**
   * Starts the master Restlet API server.
   *
   * @throws Exception if there is an error starting the server.
   */
  public void start() throws Exception {
    LOGGER.info("Starting API server");
    webServer.start();
    LOGGER.info("API server started.");
  }

  /**
   * Stops the master Restlet API server.
   *
   * @throws Exception if there is an error stopping the server.
   */
  public void stop() throws Exception {
    LOGGER.info("Stopping API server");
    webServer.shutdownNow();
    LOGGER.info("API server stopped");
  }
}
