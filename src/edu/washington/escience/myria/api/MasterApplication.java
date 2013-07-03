package edu.washington.escience.myria.api;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Context;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.servlet.WebConfig;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.wordnik.swagger.jaxrs.config.BeanConfig;

import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.parallel.Server;

/**
 * This object simply configures which resources can be requested via the REST server.
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
  @SuppressWarnings("unchecked")
  public MasterApplication(final Server server, final MasterDaemon daemon) {
    /*
     * Tell Jersey to look for resources inside the entire project, and also for Swagger.
     */
    super(new String[] { "edu.washington.escience.myria", "com.wordnik.swagger.jersey.listing" });

    /* Disable WADL - throws error messages when using Swagger, and not needed. */
    getFeatures().put(ResourceConfig.FEATURE_DISABLE_WADL, true);

    /* Whenever @Context Server or @Context MasterDaemon is used during a web request, these object will be supplied. */
    getSingletons().add(new SingletonTypeInjectableProvider<Context, Server>(Server.class, server) {
    });
    getSingletons().add(new SingletonTypeInjectableProvider<Context, MasterDaemon>(MasterDaemon.class, daemon) {
    });
    getSingletons().add(new SingletonTypeInjectableProvider<Context, ServletConfig>(ServletConfig.class, null) {
    });
    getSingletons().add(new SingletonTypeInjectableProvider<Context, WebConfig>(WebConfig.class, null) {
    });

    /* Enable Jackson's JSON Serialization/Deserialization. */
    getClasses().add(JacksonJsonProvider.class);

    /* Enable GZIP compression/decompression */
    getContainerRequestFilters().add(GZIPContentEncodingFilter.class);
    getContainerResponseFilters().add(GZIPContentEncodingFilter.class);

    /* Swagger configuration -- must come BEFORE Swagger classes are added. */
    BeanConfig myriaBeanConfig = new BeanConfig();
    myriaBeanConfig.setBasePath("http://localhost:8753");
    myriaBeanConfig.setVersion("0.1.0");
    myriaBeanConfig.setResourcePackage("edu.washington.escience.myria.api");
    myriaBeanConfig.setScan(true);

    /* Add a response filter (i.e., runs on all responses) that sets headers for cross-origin objects. */
    getContainerResponseFilters().add(new CrossOriginResponseFilter());
  }

  /**
   * This is a container response filter. It will run on all responses leaving the server and add the CORS filters
   * saying that these API calls should be allowed from any website. This is a mechanism increasingly supported by
   * modern browsers instead of, e.g., JSONP.
   * 
   * TODO revisit the security of this model
   * 
   * @author dhalperi
   * 
   */
  private class CrossOriginResponseFilter implements ContainerResponseFilter {
    @Override
    public ContainerResponse filter(final ContainerRequest request, final ContainerResponse response) {
      response.getHttpHeaders().add("Access-Control-Allow-Origin", "*");
      response.getHttpHeaders().add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
      response.getHttpHeaders().add("Access-Control-Allow-Headers", "Content-Type");
      return response;
    }
  }
}