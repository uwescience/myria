package edu.washington.escience.myriad.api;

import org.restlet.Component;
import org.restlet.data.Protocol;
import org.restlet.ext.jaxrs.JaxRsApplication;

import edu.washington.escience.myriad.parallel.Server;

/**
 * The main class for the Myria API server.
 * 
 * @author dhalperi
 * 
 */
public final class MasterApiServer {

  /* max time for waiting a server query to complete before throwing a timeout exception */
  private static final String MAX_WAITING_TIME = "86400000";
  /** The instance. */
  public static final MasterApiServer INSTANCE = new MasterApiServer();

  /** The default port for the server. */
  private static final int PORT = 8753;

  /** The Server for this application. */
  private static Server myriaServer;

  /**
   * Used by the API collection classes to access data about system internals.
   * 
   * @return the Myriad Server object that handles/knows the distributed system state.
   */
  protected static Server getMyriaServer() {
    return myriaServer;
  }

  /**
   * Private method to configure the API Server's pointer to the Myriad Server.
   * 
   * @param myriaServer a handle the the Myriad server currently running.
   */
  private static void setMyriaServer(final Server myriaServer) {
    MasterApiServer.myriaServer = myriaServer;
  }

  /**
   * Set the local handle to the Myriad Server so that we can serve API calls requesting information.
   * 
   * @param myriaServer a handle the the Myriad server currently running.
   * @throws IllegalStateException if this pointer has already been initialized.
   */
  public static void setUp(final Server myriaServer) throws IllegalStateException {
    if (MasterApiServer.getMyriaServer() != null) {
      throw new IllegalStateException("MasterServer.catalog is already initialized");
    }
    MasterApiServer.setMyriaServer(myriaServer);
  }

  /** The RESTlet Component is the main class that holds multiple servers/hosts for this application. */
  private final Component component;

  /** The RESTlet server object. */
  private final org.restlet.Server restletServer;

  /**
   * Constructor for the Master API Server.
   */
  private MasterApiServer() {
    System.setProperty("org.restlet.engine.io.timeoutMs", MAX_WAITING_TIME);
    /* create Component (as ever for Restlet) */
    component = new Component();

    /* Add a server that responds to HTTP on port PORT. */
    restletServer = component.getServers().add(Protocol.HTTP, PORT);

    /* Add a JAX-RS runtime environment, using our MasterApplication implementation. */
    final JaxRsApplication application = new JaxRsApplication();
    application.add(new MasterApplication());

    /* Attach the application to the component */
    component.getDefaultHost().attach(application);
  }

  /**
   * Starts the master RESTlet API server.
   * 
   * @throws Exception if there is an error starting the server.
   */
  public void start() throws Exception {
    System.out.println("Starting server");
    component.start();
    System.out.println("Server started on port " + restletServer.getPort());
  }

  /**
   * Stops the master RESTlet API server.
   * 
   * @throws Exception if there is an error stopping the server.
   */
  public void stop() throws Exception {
    System.out.println("Stopping server");
    component.stop();
    System.out.println("Server stopped");
  }
}