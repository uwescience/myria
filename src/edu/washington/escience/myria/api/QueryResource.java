package edu.washington.escience.myria.api;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.operator.EOSSource;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SinkRoot;
import edu.washington.escience.myria.parallel.QueryFuture;
import edu.washington.escience.myria.parallel.QueryFutureListener;
import edu.washington.escience.myria.parallel.Server;
import edu.washington.escience.myria.parallel.SingleQueryPlanWithArgs;

/**
 * Class that handles queries.
 * 
 * @author dhalperi
 */
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/query")
public final class QueryResource {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QueryResource.class);
  /** The Myria server running on the master. */
  @Context
  private Server server;

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
    query.validate();

    /* Deserialize the three arguments we need */
    Map<Integer, SingleQueryPlanWithArgs> queryPlan;
    try {
      queryPlan = query.instantiate(server);
    } catch (CatalogException e) {
      /* CatalogException means something went wrong interfacing with the Catalog. */
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    } catch (MyriaApiException e) {
      /* Passthrough MyriaApiException. */
      throw e;
    } catch (Exception e) {
      /* Other exceptions mean that the request itself was likely bad. */
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    }

    Set<Integer> usingWorkers = new HashSet<Integer>();
    usingWorkers.addAll(queryPlan.keySet());
    /* Remove the server plan if present */
    usingWorkers.remove(MyriaConstants.MASTER_ID);
    /* Make sure that the requested workers are alive. */
    if (!server.getAliveWorkers().containsAll(usingWorkers)) {
      /* Throw a 503 (Service Unavailable) */
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "Not all requested workers are alive");
    }

    SingleQueryPlanWithArgs masterPlan = queryPlan.get(MyriaConstants.MASTER_ID);
    if (masterPlan == null) {
      masterPlan = new SingleQueryPlanWithArgs(new SinkRoot(new EOSSource()));
    } else {
      queryPlan.remove(MyriaConstants.MASTER_ID);
    }
    final RootOperator masterRoot = masterPlan.getRootOps().get(0);

    /* Start the query, and get its Server-assigned Query ID */
    QueryFuture qf;
    try {
      qf = server.submitQuery(query.rawDatalog, query.logicalRa, query, masterPlan, queryPlan);
    } catch (IllegalArgumentException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, e);
    } catch (DbException | CatalogException e) {
      throw new MyriaApiException(Status.INTERNAL_SERVER_ERROR, e);
    }
    long queryId = qf.getQuery().getQueryID();
    qf.addListener(new QueryFutureListener() {

      @Override
      public void operationComplete(final QueryFuture future) throws Exception {
        if (masterRoot instanceof SinkRoot && query.expectedResultSize != null) {
          if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Expected num tuples: {}; but actually: {}", query.expectedResultSize, ((SinkRoot) masterRoot)
                .getCount());
          }
        }
      }
    });
    /* And return the queryStatus as it is now. */
    QueryStatusEncoding qs = server.getQueryStatus(queryId);
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
  @Path("query-{query_id}")
  public Response getQueryStatus(@PathParam("query_id") final long queryId, @Context final UriInfo uriInfo)
      throws CatalogException {
    final QueryStatusEncoding queryStatus = server.getQueryStatus(queryId);
    final URI uri = uriInfo.getAbsolutePath();
    if (queryStatus == null) {
      return Response.status(Status.NOT_FOUND).contentLocation(uri).entity("Query " + queryId + " was not found")
          .build();
    }
    queryStatus.url = uri;
    Status httpStatus = Status.INTERNAL_SERVER_ERROR;
    boolean cache = false;
    switch (queryStatus.status) {
      case SUCCESS:
      case ERROR:
      case KILLED:
        httpStatus = Status.OK;
        cache = true;
        break;
      case ACCEPTED:
      case PAUSED:
      case RUNNING:
        httpStatus = Status.ACCEPTED;
        break;
    }
    ResponseBuilder response = Response.status(httpStatus).location(uri).entity(queryStatus);
    if (!cache) {
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
  @Path("query-{query_id}")
  public Response cancelQuery(@PathParam("query_id") final long queryId, @Context final UriInfo uriInfo)
      throws CatalogException {
    try {
      server.killQuery(queryId);
    } catch (NullPointerException e) {
      throw new MyriaApiException(Status.BAD_REQUEST, "That query is not running.");
    }
    final QueryStatusEncoding queryStatus = server.getQueryStatus(queryId);
    final URI uri = uriInfo.getAbsolutePath();
    queryStatus.url = uri;
    ResponseBuilder response = Response.ok().location(uri).entity(queryStatus);
    return response.build();
  }

  /**
   * Get information about a query. This includes when it started, when it finished, its URL, etc.
   * 
   * @param uriInfo the URL of the current request.
   * @return information about the query.
   * @throws CatalogException if there is an error in the catalog.
   */
  @GET
  public Response getQueries(@Context final UriInfo uriInfo) throws CatalogException {
    List<QueryStatusEncoding> queries = server.getQueries();
    for (QueryStatusEncoding status : queries) {
      status.url = getCanonicalResourcePath(uriInfo, status.queryId);
    }
    return Response.ok().cacheControl(MyriaApiUtils.doNotCache()).entity(queries).build();
  }

  @GET
  @Path("id-{query_id}/qf={qf_number}&worker={worker_id}")
  public Response getProfile(@PathParam("query_id") final long queryId, @PathParam("qf_number") final long qf_number,
      @PathParam("worker_id") final long workerId, @Context final UriInfo uriInfo) {
    return Response.ok().cacheControl(MyriaApiUtils.doNotCache()).entity("Hi there").build();
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