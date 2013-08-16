package edu.washington.escience.myria.api;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
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
   */
  @POST
  public Response postNewQuery(final QueryEncoding query, @Context final UriInfo uriInfo) {
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
      throw e;
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
      qf = server.submitQuery(query.rawDatalog, query.logicalRa, masterPlan, queryPlan);
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
    /* In the response, tell the client what ID this query was assigned. */
    UriBuilder queryUri = uriInfo.getAbsolutePathBuilder();
    return Response.created(queryUri.path("query-" + queryId).build()).entity(query).build();
  }
}