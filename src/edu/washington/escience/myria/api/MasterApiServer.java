package edu.washington.escience.myria.api;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Inet4Address;
import java.net.URI;
import java.net.URL;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.ResourceConfig;

import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.tool.MyriaConfigurationReader;

/**
 * The main class for the Myria API server.
 * 
 * @author dhalperi
 * 
 */
public final class MasterApiServer {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterApiServer.class);

  /** The Jersey web server. */
  private final HttpServer webServer;

  /**
   * Constructor for the Master API Server.
   * 
   * @param server the Myria server that will handle API requests.
   * @param daemon the Myria master daemon.
   * @param port the port the Myria API server will listen on.
   * @throws IOException if the server cannot be created.
   */
  public MasterApiServer(final Server server, final MasterDaemon daemon, final int port) throws IOException {
    URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(port).build();
    ResourceConfig masterApplication = new MasterApplication(server, daemon);
    webServer = GrizzlyServerFactory.createHttpServer(baseUri, masterApplication);

    registerToWatchdog(server, port);
  }

  /**
   * Register this deployment to the watchdog.
   * 
   * @param server the Myria server that will handle API requests.
   * @param port the port the Myria API server will listen on.
   * @throws IOException if registration failed.
   */
  public void registerToWatchdog(final Server server, final int port) throws IOException {
    final String deploymentFile = server.getConfiguration(MyriaSystemConfigKeys.DEPLOYMENT_FILE);
    final String description = server.getConfiguration(MyriaSystemConfigKeys.DESCRIPTION);
    final String workingDir =
        server.getConfiguration(MyriaSystemConfigKeys.WORKING_DIRECTORY) + "/" + description + "-files/" + description;
    final String masterApiSocket = Inet4Address.getLocalHost().getHostAddress() + ":" + port;

    final Map<String, Map<String, String>> config = new MyriaConfigurationReader().load(deploymentFile);
    final String watchdog = config.get("deployment").get("watchdog");
    String secretCode = config.get("deployment").get("secret_code");
    if (secretCode == null) {
      secretCode = "";
    }
    if (watchdog != null) {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Registering myself to the watchdog " + watchdog);
      }

      String urlParameters =
          "master=" + masterApiSocket + "&" + "working_dir=" + workingDir + "&" + "deployment_file=" + deploymentFile
              + "&" + "secret_code=" + secretCode;
      HttpURLConnection connection = null;
      try {
        URL url = new URL("http://" + watchdog + "/register");
        connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setDoInput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setRequestProperty("Content-Length", "" + urlParameters.getBytes().length);
        connection.setUseCaches(false);

        DataOutputStream output = new DataOutputStream(connection.getOutputStream());
        output.writeBytes(urlParameters);
        output.flush();
        output.close();
        if (connection.getResponseCode() != 200) {
          throw new RuntimeException("return code: " + connection.getResponseCode());
        }
      } catch (Exception e) {
        LOGGER.error("Cannot POST to the watchdog, cause: " + e);
      } finally {
        connection.disconnect();
      }
    }
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
    webServer.stop();
    LOGGER.info("API server stopped");
  }
}