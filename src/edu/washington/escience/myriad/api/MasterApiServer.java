package edu.washington.escience.myriad.api;

import java.util.concurrent.TimeUnit;

import org.restlet.Component;
import org.restlet.Context;
import org.restlet.data.Protocol;
import org.restlet.ext.jaxrs.JaxRsApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.daemon.MasterDaemon;
import edu.washington.escience.myriad.parallel.Server;

/**
 * The main class for the Myria API server.
 * 
 * @author dhalperi
 * 
 */
public final class MasterApiServer {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterApiServer.class);

  /** Max time for waiting a server query to complete before throwing a timeout exception is 1 day. */
  private static final long MAX_WAITING_TIME_MS = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS);

  /** The default port for the server. */
  private static final int PORT = 8753;

  /** The Restlet Component is the main class that holds multiple servers/hosts for this application. */
  private final Component component;

  /** The Restlet server object. */
  private final org.restlet.Server restletServer;

  /**
   * Constructor for the Master API Server.
   * 
   * @param server the Myria server that will handle API requests.
   * @param daemon the Myria master daemon.
   */
  public MasterApiServer(final Server server, final MasterDaemon daemon) {
    /* Set API requests to time out after MAX_WAITING_TIME_MS milliseconds */
    System.setProperty("org.restlet.engine.io.timeoutMs", Long.toString(MAX_WAITING_TIME_MS));

    /* create Component (as ever for Restlet) */
    component = new Component();

    /* Add a server that responds to HTTP on port PORT. */
    restletServer = component.getServers().add(Protocol.HTTP, PORT);

    /* Setup the context variables that will be available to all Restlets. */
    Context context = component.getContext().createChildContext(); // Child context for security.
    context.getAttributes().put(MyriaApiConstants.MYRIA_SERVER_ATTRIBUTE, server);
    context.getAttributes().put(MyriaApiConstants.MYRIA_MASTER_DAEMON_ATTRIBUTE, daemon);

    /* Add a JAX-RS runtime environment, using our MasterApplication implementation. */
    final JaxRsApplication application = new JaxRsApplication(context);
    application.add(new MasterApplication());

    /* Attach the application to the component */
    component.getDefaultHost().attach(application);
  }

  /**
   * Starts the master Restlet API server.
   * 
   * @throws Exception if there is an error starting the server.
   */
  public void start() throws Exception {
    System.out.println("Starting API server");
    component.start();
    System.out.println("API server started on port " + restletServer.getPort());
  }

  /**
   * Stops the master Restlet API server.
   * 
   * @throws Exception if there is an error stopping the server.
   */
  public void stop() throws Exception {
    LOGGER.info("Stopping API server");
    component.stop();
    LOGGER.info("API server stopped");
  }
}