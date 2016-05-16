package edu.washington.escience.myria.api;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.ws.rs.NameBinding;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.internal.util.Base64;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.message.GZipEncoder;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.filter.EncodingFilter;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.wordnik.swagger.jaxrs.config.BeanConfig;

import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.parallel.Server;

/**
 * This object simply configures which resources can be requested via the REST server.
 *
 */
public final class MasterApplication extends ResourceConfig {

  /**
   * Instantiate the main application running on the Myria master.
   *
   * @param server the Myria server running on this master.
   * @param daemon the Myria daemon running on this master.
   */
  public MasterApplication(final Server server, final MasterDaemon daemon) {
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
            bind(server).to(Server.class);
            bind(daemon).to(MasterDaemon.class);
          }
        });

    /* Enable GZIP compression/decompression */
    register(EncodingFilter.class);
    register(GZipEncoder.class);

    /* Swagger configuration -- must come BEFORE Swagger classes are added. */
    BeanConfig myriaBeanConfig = new BeanConfig();
    /*
     * TODO(dhalperi): make this more dynamic based on either Catalog or runtime option.
     */
    myriaBeanConfig.setBasePath("http://rest.myria.cs.washington.edu:1776");
    myriaBeanConfig.setVersion("0.1.0");
    myriaBeanConfig.setResourcePackage("edu.washington.escience.myria.api");
    myriaBeanConfig.setScan(true);

    /* Add a response filter (i.e., runs on all responses) that sets headers for cross-origin objects. */
    register(new CrossOriginResponseFilter());
    /* Add an admin authentication filter. */
    register(new AdminAuthFilter());
  }

  /**
   * This is a container response filter. It will run on all responses leaving the server and add the CORS filters
   * saying that these API calls should be allowed from any website. This is a mechanism increasingly supported by
   * modern browsers instead of, e.g., JSONP.
   *
   * For more information, visit http://www.w3.org/TR/cors/ and http://enable-cors.org/
   *
   * TODO revisit the security of this model
   *
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

  /**
   * Annotation that indicates a REST API requires admin authentication. Only filters annotated with the same annotation
   * will be invoked.
   */
  @NameBinding
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ADMIN {}

  /**
   * Check the admin authentication information.
   */
  @ADMIN
  private class AdminAuthFilter implements ContainerRequestFilter {
    /** the server. */
    @Context private Server server;

    @Override
    public void filter(final ContainerRequestContext request) {
      String adminPassword =
          server.getConfig().getOptional("deployment", MyriaSystemConfigKeys.ADMIN_PASSWORD);
      if (adminPassword == null) {
        /* No admin password is required, pass. */
        return;
      }
      String authentication = request.getHeaderString(ContainerRequest.AUTHORIZATION);
      if (authentication == null || !authentication.startsWith("Basic ")) {
        /* No valid basic auth information. Return 401 to let the browser pop up a dialog. */
        throw new WebApplicationException(
            Response.status(Status.UNAUTHORIZED)
                .header("WWW-Authenticate", "Basic realm=\"Myria admin credentials\"")
                .entity("No basic authentication information provided.")
                .build());
      }
      authentication = authentication.substring("Basic ".length());
      String[] values = new String(Base64.decodeAsString(authentication)).split(":");
      if (values.length != 2) {
        throw new MyriaApiException(
            Status.BAD_REQUEST,
            "Bad format. Usage: a string \"username:password\" encoded in Base64");
      }
      String username = values[0];
      String password = values[1];
      if (userIsAdmin(username)) {
        /* User says it's admin. */
        if (!password.equals(adminPassword)) {
          /* But the password is incorrect. */
          throw new MyriaApiException(Status.UNAUTHORIZED, "Admin password incorrect");
        }
        /* Otherwise, pass! */
      } else {
        /* Otherwise it's a normal user so FORBIDDEN */
        throw new MyriaApiException(
            Status.FORBIDDEN, "Please provide admin authentication information.");
      }
    }

    /**
     * Check if the given user is an admin.
     *
     * @param username the user name.
     * @return if the given user is an admin.
     */
    private boolean userIsAdmin(final String username) {
      /* Temporary approach: only true when username.toLowerCase() is "admin" */
      return username.toLowerCase().equals("admin");
    }
  }
}
