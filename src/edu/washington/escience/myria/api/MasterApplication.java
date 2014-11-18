package edu.washington.escience.myria.api;

import java.security.Principal;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.container.filter.RolesAllowedResourceFilterFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.servlet.WebConfig;
import com.sun.jersey.spi.inject.SingletonTypeInjectableProvider;
import com.wordnik.swagger.jaxrs.config.BeanConfig;

import edu.washington.escience.myria.MyriaConstants.ROLE;
import edu.washington.escience.myria.MyriaSystemConfigKeys;
import edu.washington.escience.myria.daemon.MasterDaemon;
import edu.washington.escience.myria.parallel.Server;

/**
 * This object simply configures which resources can be requested via the REST server.
 * 
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
    /* TODO(dhalperi): make this more dynamic based on either Catalog or runtime option. */
    myriaBeanConfig.setBasePath("http://rest.myria.cs.washington.edu:1776");
    myriaBeanConfig.setVersion("0.1.0");
    myriaBeanConfig.setResourcePackage("edu.washington.escience.myria.api");
    myriaBeanConfig.setScan(true);

    /* Add a response filter (i.e., runs on all responses) that sets headers for cross-origin objects. */
    getContainerResponseFilters().add(new CrossOriginResponseFilter());

    getContainerRequestFilters().add(new AuthenticateFilter());
    /* TODO(jwang): change the class if jersey is upgraded to 2.x */
    getResourceFilterFactories().add(RolesAllowedResourceFilterFactory.class.getName());
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

  /**
   * Check the authentication information and set roles. Currently there are only two roles: "admin" and "user". By
   * default the role is set to "user" unless the authentication specifies "admin" with the correct password.
   * 
   */
  private class AuthenticateFilter implements ContainerRequestFilter {
    /** UriInfo. */
    @Context
    private UriInfo uriInfo;
    /** the server. */
    @Context
    private Server server;

    @Override
    public ContainerRequest filter(final ContainerRequest request) {
      request.setSecurityContext(new Authorizer(ROLE.user));
      String authentication = request.getHeaderValue(ContainerRequest.AUTHORIZATION);
      if (authentication != null && authentication.startsWith("Basic ")) {
        authentication = authentication.substring("Basic ".length());
        String[] values = new String(Base64.base64Decode(authentication)).split(":");
        if (values.length != 2 || values[0] == null || values[1] == null) {
          /* Bad format. */
          throw new MyriaApiException(Status.BAD_REQUEST,
              "Bad format. Usage: a string \"username:password\" encoded in Base64");
        }
        if (values[0].toLowerCase().equals(ROLE.admin)) {
          /* User says it's admin. */
          if (!checkAdminPassword(values[1])) {
            /* But the password is incorrect. */
            throw new MyriaApiException(Status.UNAUTHORIZED, "Admin password incorrect");
          }
          /* Correct, set its role to be admin. */
          request.setSecurityContext(new Authorizer(ROLE.admin));
        }
      }
      return request;
    }

    /**
     * Check if the user is an admin with a correct password.
     * 
     * @param passwd password.
     * @return if the authentication succeeded.
     */
    boolean checkAdminPassword(final String passwd) {
      return passwd.equals(server.getConfiguration(MyriaSystemConfigKeys.ADMIN_PASSWORD));
    }

    /**
     * The customized SecurityContext.
     */
    private class Authorizer implements SecurityContext {

      /** user name. */
      private ROLE user;
      /** principal. */
      private Principal principal;

      /**
       * Constructor.
       * 
       * @param user username.
       * */
      Authorizer(final ROLE user) {
        this.user = user;
        principal = new Principal() {
          @Override
          public String getName() {
            return user.toString();
          }
        };
      }

      @Override
      public Principal getUserPrincipal() {
        return principal;
      }

      @Override
      public boolean isUserInRole(final String role) {
        return (role.equals(user));
      }

      @Override
      public String getAuthenticationScheme() {
        return SecurityContext.BASIC_AUTH;
      }

      @Override
      public boolean isSecure() {
        return "https".equals(uriInfo.getRequestUri().getScheme());
      }
    }
  }
}
