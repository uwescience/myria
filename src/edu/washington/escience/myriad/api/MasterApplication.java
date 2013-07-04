package edu.washington.escience.myriad.api;

import javax.ws.rs.core.Context;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;

import edu.washington.escience.myriad.daemon.MasterDaemon;
import edu.washington.escience.myriad.parallel.Server;

/**
 * This object simply configures which resources can be requested via the Restlet server.
 * 
 * @author dhalperi, jwang
 */
public final class MasterApplication extends PackagesResourceConfig {

  /**
   * Instantiate the main application running on the Myria master.
   * 
   * @param server the Myria server running on this master.
   * @param daemon the Myria daemon running on this master.
   */
  public MasterApplication(final Server server, final MasterDaemon daemon) {
    /* Tell Jersey to look for resources inside the entire project. */
    super("edu.washington.escience.myriad");
    /* Whenever @Context Server or @Context MasterDaemon is used during a web request, these object will be supplied. */
    getSingletons().add(new SingletonTypeInjectableProvider<Context, Server>(Server.class, server) {
    });
    getSingletons().add(new SingletonTypeInjectableProvider<Context, MasterDaemon>(MasterDaemon.class, daemon) {
    });
    /* Enable Jackson's JSON Serialization/Deserialization. */
    getClasses().add(JacksonJsonProvider.class);
  }
}