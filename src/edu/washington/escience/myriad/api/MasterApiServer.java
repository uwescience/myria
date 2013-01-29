package edu.washington.escience.myriad.api;

import org.restlet.Component;
import org.restlet.data.Protocol;
import org.restlet.ext.jaxrs.JaxRsApplication;

import edu.washington.escience.myriad.parallel.Server;

public final class MasterApiServer {

  /** The instance. */
  public static final MasterApiServer INSTANCE = new MasterApiServer();

  /** The default port for the server. */
  private static final int PORT = 8753;

  /** The Server for this application. */
  private static Server myriaServer;

  public static Server getMyriaServer() {
    return myriaServer;
  }

  private static void setMyriaServer(final Server myriaServer) {
    MasterApiServer.myriaServer = myriaServer;
  }

  public static void setUp(final Server myriaServer) throws Exception {
    if (MasterApiServer.getMyriaServer() != null) {
      throw new IllegalStateException("MasterServer.catalog is already initialized");
    }
    MasterApiServer.setMyriaServer(myriaServer);
  }

  /** The Reslet Component is the main class that holds multiple servers/hosts for this application. */
  private final Component component;

  /** The RESTlet server object. */
  private final org.restlet.Server restletServer;

  private MasterApiServer() {
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

  public void start() throws Exception {
    System.out.println("Starting server");
    component.start();
    System.out.println("Server started on port " + restletServer.getPort());
  }

  public void stop() throws Exception {
    System.out.println("Stopping server");
    component.stop();
    System.out.println("Server stopped");
  }
}