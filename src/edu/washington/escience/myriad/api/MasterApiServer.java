package edu.washington.escience.myriad.api;

import org.restlet.Component;
import org.restlet.Server;
import org.restlet.data.Protocol;
import org.restlet.ext.jaxrs.JaxRsApplication;

import edu.washington.escience.myriad.coordinator.catalog.Catalog;

public final class MasterApiServer {

  /** The instance. */
  public static final MasterApiServer INSTANCE = new MasterApiServer();

  /** The default port for the server. */
  private static final int PORT = 8753;

  /** The Catalog for this application. */
  private static Catalog catalog;

  /** The Reslet Component is the main class that holds multiple servers/hosts for this application. */
  private final Component component;

  /** The RESTlet server object. */
  private final Server server;

  private MasterApiServer() {
    /* create Component (as ever for Restlet) */
    component = new Component();

    /* Add a server that responds to HTTP on port PORT. */
    server = component.getServers().add(Protocol.HTTP, PORT);

    /* Add a JAX-RS runtime environment, using our MasterApplication implemention. */
    JaxRsApplication application = new JaxRsApplication();
    application.add(new MasterApplication());

    /* Attach the application to the component */
    component.getDefaultHost().attach(application);
  }

  public static void setUp(final Catalog catalog) throws Exception {
    if (MasterApiServer.getCatalog() != null) {
      throw new IllegalStateException("MasterServer.catalog is already initialized");
    }
    MasterApiServer.setCatalog(catalog);
  }

  public void start() throws Exception {
    System.out.println("Starting server");
    component.start();
    System.out.println("Server started on port " + server.getPort());
  }

  public void stop() throws Exception {
    System.out.println("Stopping server");
    component.stop();
    getCatalog().close();
    System.out.println("Server stopped");
  }

  public static Catalog getCatalog() {
    return catalog;
  }

  private static void setCatalog(Catalog catalog) {
    MasterApiServer.catalog = catalog;
  }
}