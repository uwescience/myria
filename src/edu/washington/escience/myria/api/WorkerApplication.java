package edu.washington.escience.myria.api;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import edu.washington.escience.myria.parallel.Worker;

/**
 * This object simply configures which resources can be requested via the REST server.
 *
 */
public final class WorkerApplication extends ResourceConfig {

  /**
   * Instantiate the worker application running on the Myria worker.
   *
   * @param worker the worker instance.
   */
  public WorkerApplication(final Worker worker) {
    /*
     * Tell Jersey to look for resources inside the entire project, and also for Swagger.
     */
    packages(new String[] {"edu.washington.escience.myria", "com.wordnik.swagger.jersey.listing"});

    /*
     * Disable WADL - throws error messages when using Swagger, and not needed.
     */
    property(ServerProperties.WADL_FEATURE_DISABLE, true);

    /* Enable Jackson's JSON Serialization/Deserialization. */
    register(JacksonJsonProvider.class);

    /* Enable Multipart. */
    register(MultiPartFeature.class);

    /* Register the singleton binder. */
    register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            /* Singletons binding. */
            bind(worker).to(Worker.class);
          }
        });
    /*
     * Add a response filter (i.e., runs on all responses) that sets headers for cross-origin
     * objects.
     */
    register(new CrossOriginResponseFilter());
  }

  /**
   * This is a container response filter. It will run on all responses leaving the server and add
   * the CORS filters saying that these API calls should be allowed from any website. This is a
   * mechanism increasingly supported by modern browsers instead of, e.g., JSONP.
   *
   * For more information, visit http://www.w3.org/TR/cors/ and http://enable-cors.org/
   *
   * TODO revisit the security of this model
   */
  private class CrossOriginResponseFilter implements ContainerResponseFilter {
    @Override
    public void filter(
        final ContainerRequestContext request, final ContainerResponseContext response) {
      response.getHeaders().add("Access-Control-Allow-Origin", "*");
      response.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
      response.getHeaders().add("Access-Control-Allow-Headers", "Content-Type");
    }
  }
}
