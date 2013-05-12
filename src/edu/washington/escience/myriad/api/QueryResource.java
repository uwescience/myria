package edu.washington.escience.myriad.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.api.encoding.OperatorEncoding;
import edu.washington.escience.myriad.api.encoding.QueryEncoding;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.EOSSource;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.operator.SinkRoot;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.QueryFuture;
import edu.washington.escience.myriad.parallel.QueryFutureListener;

/**
 * Class that handles queries.
 * 
 * @author dhalperi
 */
@Path("/query")
public final class QueryResource {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QueryResource.class);

  /**
   * For now, simply echoes back its input.
   * 
   * @param payload the payload of the POST request itself.
   * @param uriInfo the URI of the current request.
   * @return the URI of the created query.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response postNewQuery(final byte[] payload, @Context final UriInfo uriInfo) {
    final QueryEncoding query = MyriaApiUtils.deserialize(payload, QueryEncoding.class);

    /* Deserialize the three arguments we need */
    Map<Integer, RootOperator[]> queryPlan = instantiateQueryPlan(query.queryPlan);

    Set<Integer> usingWorkers = new HashSet<Integer>();
    usingWorkers.addAll(queryPlan.keySet());
    /* Remove the server plan if present */
    usingWorkers.remove(MyriaConstants.MASTER_ID);
    /* Make sure that the requested workers are alive. */
    if (!MyriaApiUtils.getServer().getAliveWorkers().containsAll(usingWorkers)) {
      /* Throw a 503 (Service Unavailable) */
      throw new MyriaApiException(Status.SERVICE_UNAVAILABLE, "Not all requested workers are alive");
    }

    RootOperator[] masterPlan = queryPlan.get(MyriaConstants.MASTER_ID);
    if (masterPlan == null) {
      masterPlan = new RootOperator[] { new SinkRoot(new EOSSource()) };
      queryPlan.put(MyriaConstants.MASTER_ID, masterPlan);
    }
    final RootOperator masterRoot = masterPlan[0];

    /* Start the query, and get its Server-assigned Query ID */
    QueryFuture qf;
    try {
      qf = MyriaApiUtils.getServer().submitQuery(query.rawDatalog, query.logicalRa, queryPlan);
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
    return Response.created(queryUri.path("query-" + queryId).build()).build();
  }

  /**
   * Deserialize the mapping of worker subplans to workers.
   * 
   * @param queryPlan a Myria query plan encoding.
   * @return the query plan.
   */
  private static Map<Integer, RootOperator[]> instantiateQueryPlan(
      final Map<Integer, List<List<OperatorEncoding<?>>>> queryPlan) {
    Map<Integer, RootOperator[]> ret = new HashMap<Integer, RootOperator[]>();
    for (Entry<Integer, List<List<OperatorEncoding<?>>>> entry : queryPlan.entrySet()) {
      Integer workerId = entry.getKey();
      List<List<OperatorEncoding<?>>> workerPlan = entry.getValue();
      ret.put(workerId, instantiateWorkerPlan(workerPlan));
    }
    return ret;
  }

  /**
   * Given an encoding of a worker's slice of the plan (i.e., its list of plan fragments), instantiate the actual
   * operators of the plan.
   * 
   * @param workerPlan the encoding of the worker's plan.
   * @return the actual plan.
   */
  private static RootOperator[] instantiateWorkerPlan(final List<List<OperatorEncoding<?>>> workerPlan) {
    RootOperator[] ret = new RootOperator[workerPlan.size()];
    int i = 0;
    for (List<OperatorEncoding<?>> planFragment : workerPlan) {
      ret[i] = instantiatePlanFragment(planFragment);
      i++;
    }
    return ret;
  }

  /**
   * Given an encoding of a plan fragment, i.e., a connected list of operators, instantiate the actual plan fragment.
   * This includes instantiating the operators and connecting them together. The constraint on the plan fragments is
   * that the last operator in the fragment must be the RootOperator. There is a special exception for older plans in
   * which a CollectConsumer will automatically have a SinkRoot appended to it.
   * 
   * @param planFragment the encoded plan fragment.
   * @return the actual plan fragment.
   */
  private static RootOperator instantiatePlanFragment(final List<OperatorEncoding<?>> planFragment) {
    Map<String, Operator> operators = new HashMap<String, Operator>();

    /* Instantiate all the operators. */
    for (OperatorEncoding<?> encoding : planFragment) {
      operators.put(encoding.opName, encoding.construct());
    }
    /* Connect all the operators. */
    for (OperatorEncoding<?> encoding : planFragment) {
      encoding.connect(operators.get(encoding.opName), (operators));
    }
    /* Return the first one. */
    Operator ret = operators.get(planFragment.get(planFragment.size() - 1).opName);
    if (ret instanceof RootOperator) {
      return (RootOperator) ret;
    } else if (ret instanceof CollectConsumer) {
      /* Old query plan, add a SinkRoot to the top. */
      SinkRoot sinkRoot = new SinkRoot(ret);
      return sinkRoot;
    } else {
      throw new MyriaApiException(Status.BAD_REQUEST,
          "The last operator in a plan fragment must be a RootOperator, not " + ret.getClass());
    }
  }
}