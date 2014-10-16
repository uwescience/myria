package edu.washington.escience.myria.api;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.Server;

/**
 * Class that handles queries.
 * 
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/query")
public final class QueryResource {
  /** The Myria server running on the master. */
  @Context
  private Server server;

  /**
   * For testing purposes, simply deserialize the submitted query and instantiate it with physical parameters (specific
   * machines, etc.). Return the result.
   * 
   * @param query the query to be validated.
   * @param uriInfo the URI of the current request.
   * @return the deserialized query.
   */
  @POST
  @Path("validate")
  public Response validateQuery(final QueryEncoding query, @Context final UriInfo uriInfo) {
    /* Validate the input. */
    Preconditions.checkArgument(query != null, "Missing query encoding.");

    query.validate();
    /* Make sure we can serialize it properly. */
    try {
      MyriaJsonMapperProvider.getWriter().writeValueAsString(query);
    } catch (JsonProcessingException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, new IOException(
          "Unable to re-serialize the validated query", e));
    }
    /* Send back the deserialized query. */
    return Response.ok(query).build();
  }

  /**
   * For now, simply echoes back its input.
   * 
   * @param query the query to be executed.
   * @param uriInfo the URI of the current request.
   * @return the URI of the created query.
   * @throws CatalogException if there is an error in the catalog.
   */
  @POST
  public Response postNewQuery(final QueryEncoding query, @Context final UriInfo uriInfo) throws CatalogException {
    /* Validate the input. */
    Preconditions.checkArgument(query != null, "Missing query encoding.");
    query.validate();

    /* Start the query, and get its Server-assigned Query ID */
    QueryFuture qf;
    try {
      qf = server.getQueryManager().submitQuery(query, query.plan.getPlan());
    } catch (MyriaApiException e) {
      /* Passthrough MyriaApiException. */
      throw e;
    } catch (CatalogException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    } catch (Throwable e) {
      /* Other exceptions mean that the request itself was likely bad. */
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    /* Check to see if the query was submitted successfully. */
    if (qf == null) {
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "The server cannot accept new queries right now.");
    }

    long queryId = qf.getQueryId();
    /* And return the queryStatus as it is now. */
    QueryStatusEncoding qs = server.getQueryManager().getQueryStatus(queryId);
    URI queryUri = getCanonicalResourcePath(uriInfo, queryId);
    qs.url = queryUri;
    return Response.status(Status.ACCEPTED).cacheControl(MyriaApiUtils.doNotCache()).location(queryUri).entity(qs)
        .build();
  }

  /**
   * Get information about a query. This includes when it started, when it finished, its URL, etc.
   * 
   * @param queryId the query id.
   * @param uriInfo the URL of the current request.
   * @return information about the query.
   * @throws CatalogException if there is an error in the catalog.
   */
  @GET
  @Path("query-{queryId:\\d+}")
  public Response getQueryStatus(@PathParam("queryId") final long queryId, @Context final UriInfo uriInfo)
      throws CatalogException {
    final QueryStatusEncoding queryStatus = server.getQueryManager().getQueryStatus(queryId);
    final URI uri = uriInfo.getAbsolutePath();
    if (queryStatus == null) {
      return Response.status(Status.NOT_FOUND).contentLocation(uri).entity("Query " + queryId + " was not found")
          .build();
    }
    queryStatus.url = uri;
    Status httpStatus = Status.INTERNAL_SERVER_ERROR;
    switch (queryStatus.status) {
      case SUCCESS:
      case ERROR:
      case KILLED:
      case UNKNOWN:
        httpStatus = Status.OK;
        break;
      case ACCEPTED:
      case RUNNING:
      case KILLING:
        httpStatus = Status.ACCEPTED;
        break;
    }
    ResponseBuilder response = Response.status(httpStatus).location(uri).entity(queryStatus);
    if (!QueryStatusEncoding.Status.finished(queryStatus.status)) {
      response.cacheControl(MyriaApiUtils.doNotCache());
    }
    return response.build();
  }

  /**
   * Cancel a running query.
   * 
   * @param queryId the query id.
   * @param uriInfo the URL of the current request.
   * @return whether the cancellation succeeded.
   * @throws CatalogException if there is an error in the catalog.
   */
  @DELETE
  @Path("query-{queryId:\\d+}")
  public Response cancelQuery(@PathParam("queryId") final long queryId, @Context final UriInfo uriInfo)
      throws CatalogException {
    try {
      server.getQueryManager().killQuery(queryId);
    } catch (NullPointerException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "That query is not running.");
    }
    final QueryStatusEncoding queryStatus = server.getQueryManager().getQueryStatus(queryId);
    final URI uri = uriInfo.getAbsolutePath();
    queryStatus.url = uri;
    ResponseBuilder response = Response.ok().location(uri).entity(queryStatus);
    return response.build();
  }

  /**
   * Get information about a query. This includes when it started, when it finished, its URL, etc.
   * 
   * @param uriInfo the URL of the current request.
   * @param limit the maximum number of results to return. If null, the default of
   *          {@link MyriaApiConstants.MYRIA_API_DEFAULT_NUM_RESULTS} is used. Any value <= 0 is interpreted as all
   *          results.
   * @param maxId the largest query ID returned. If null or <= 0, all queries will be returned.
   * @return information about the query.
   * @throws CatalogException if there is an error in the catalog.
   */
  @GET
  public Response getQueries(@Context final UriInfo uriInfo, @QueryParam("limit") final Long limit,
      @QueryParam("max") final Long maxId) throws CatalogException {
    long realLimit = Objects.firstNonNull(limit, MyriaApiConstants.MYRIA_API_DEFAULT_NUM_RESULTS);
    long realMaxId = Objects.firstNonNull(maxId, 0L);
    List<QueryStatusEncoding> queries = server.getQueryManager().getQueries(realLimit, realMaxId);
    for (QueryStatusEncoding status : queries) {
      status.url = getCanonicalResourcePath(uriInfo, status.queryId);
    }
    return Response.ok().cacheControl(MyriaApiUtils.doNotCache()).header("X-Count", server.getNumQueries()).entity(
        queries).build();
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @return the canonical URL for this API.
   */
  public static URI getCanonicalResourcePath(final UriInfo uriInfo) {
    return getCanonicalResourcePathBuilder(uriInfo).build();
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @return a builder for the canonical URL for this API.
   */
  public static UriBuilder getCanonicalResourcePathBuilder(final UriInfo uriInfo) {
    return uriInfo.getBaseUriBuilder().path("/query");
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @param queryId the id of the query.
   * @return the canonical URL for this API.
   */
  public static URI getCanonicalResourcePath(final UriInfo uriInfo, final long queryId) {
    return getCanonicalResourcePathBuilder(uriInfo, queryId).build();
  }

  /**
   * @param uriInfo information about the URL of the request.
   * @param queryId the id of the query.
   * @return a builder for the canonical URL for this API.
   */
  public static UriBuilder getCanonicalResourcePathBuilder(final UriInfo uriInfo, final long queryId) {
    return getCanonicalResourcePathBuilder(uriInfo).path("query-" + queryId);
  }
}
