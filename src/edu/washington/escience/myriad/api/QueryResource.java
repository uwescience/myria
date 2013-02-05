package edu.washington.escience.myriad.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;

/**
 * Class that handles queries.
 * 
 * @author dhalperi
 */
@Path("/query")
public final class QueryResource {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(QueryResource.class.getName());

  /**
   * For now, simply echoes back its input.
   * 
   * @param input the payload of the POST request itself.
   * @param uriInfo the URI of the current request.
   * @return the payload.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response postNewQuery(final String input, @Context final UriInfo uriInfo) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      final Map<?, ?> userData;
      userData = mapper.readValue(input, Map.class);
      /* Must contain the raw data, the logical_ra, and the query_plan. */
      if (!userData.containsKey("raw_datalog") || !userData.containsKey("logical_ra")
          || !userData.containsKey("query_plan")) {
        LOGGER.warn("required fields: raw_datalog, logical_ra, and query_plan");
        throw new WebApplicationException(Response.status(Status.BAD_REQUEST).entity(
            "required fields: raw_datalog, logical_ra, and query_plan").build());
      }
      /* Deserialize the three arguments we need */
      final String rawQuery = (String) userData.get("raw_datalog");
      final String logicalRa = (String) userData.get("logical_ra");
      Map<Integer, Operator[]> queryPlan = deserializeJsonQueryPlan(userData.get("query_plan"));
      /* Start the query, and get its Server-assigned Query ID */
      final Long queryId = MasterApiServer.getMyriaServer().startQuery(rawQuery, logicalRa, queryPlan);
      /* In the response, tell the client what ID this query was assigned. */
      UriBuilder queryUri = uriInfo.getAbsolutePathBuilder();
      return Response.created(queryUri.path("query-" + queryId.toString()).build()).build();
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      LOGGER.warn(e.toString());
      throw new WebApplicationException(Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build());
    }
  }

  /**
   * Deserialize the mapping of worker subplans to workers.
   * 
   * @param jsonQuery a JSON object that contains a Myria query plan.
   * @return the query plan.
   * @throws Exception in the event of a bad plan.
   */
  private static Map<Integer, Operator[]> deserializeJsonQueryPlan(final Object jsonQuery) throws Exception {
    /* Better be a map */
    if (!(jsonQuery instanceof Map)) {
      throw new ClassCastException("argument is not a Map");
    }
    Map<?, ?> jsonQueryPlan = (Map<?, ?>) jsonQuery;
    Map<Integer, Operator[]> ret = new HashMap<Integer, Operator[]>();
    for (Entry<?, ?> entry : jsonQueryPlan.entrySet()) {
      Integer workerId = Integer.parseInt((String) entry.getKey());
      Operator[] workerPlan = deserializeJsonLocalPlans(entry.getValue());
      ret.put(workerId, workerPlan);
    }
    return ret;
  }

  private static Operator[] deserializeJsonLocalPlans(Object jsonLocalPlanList) throws Exception {
    /* Better be a List */
    if (!(jsonLocalPlanList instanceof List)) {
      throw new ClassCastException("argument is not a List of Operator definitions.");
    }
    List<?> localPlanList = (List<?>) jsonLocalPlanList;
    Operator[] ret = new Operator[localPlanList.size()];
    int i = 0;
    for (Object o : localPlanList) {
      ret[i] = deserializeJsonLocalPlan(o);
    }
    return ret;
  }

  private static Operator deserializeJsonLocalPlan(Object jsonLocalPlan) throws Exception {
    /* Better be a List */
    if (!(jsonLocalPlan instanceof List)) {
      throw new ClassCastException("argument is not a List of Operator definitions.");
    }
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> jsonOperators = (List<Map<String, Object>>) jsonLocalPlan;
    /* Better have at least one operator. */
    if (jsonOperators.isEmpty()) {
      throw new IOException("worker plan must contain at least one operator.");
    }
    final Map<String, Operator> operators = new HashMap<String, Operator>();
    Operator op = null;
    for (Map<String, Object> jsonOperator : jsonOperators) {
      /* Better have an operator name. */
      String opName = (String) jsonOperator.get("op_name");
      Objects.requireNonNull(opName, "all Operators must have an op_name defined");
      op = deserializeJsonOperator(jsonOperator, operators);
      operators.put(opName, op);
    }
    return op;
  }

  private static Operator deserializeJsonOperator(final Map<String, Object> jsonOperator,
      final Map<String, Operator> operators) throws Exception {
    /* Better have an operator type */
    String opType = (String) jsonOperator.get("op_type");
    Objects.requireNonNull(opType, "all Operators must have an op_type defined");

    /* Generic variable names */
    String childName;
    String child2Name;
    String relationName;
    Operator child;
    Operator child2;
    int i;

    /* Do the case-by-case work. */
    switch (opType) {
      case "SQLiteInsert":
        childName = (String) jsonOperator.get("arg_child");
        Objects.requireNonNull(childName, "missing SQLiteInsert field: arg_child");
        child = operators.get(childName);
        Objects.requireNonNull(child, "SQLiteInsert child Operator " + childName + " not previously defined");
        relationName = (String) jsonOperator.get("arg_relation_name");
        Objects.requireNonNull(relationName, "missing SQLiteInsert field: arg_relation_name");
        if (jsonOperator.containsKey("arg_overwrite_table")) {
          Boolean overwrite = Boolean.parseBoolean((String) jsonOperator.get("arg_overwrite_table"));
          return new SQLiteInsert(child, relationName, null, null, overwrite);
        }
        return new SQLiteInsert(child, relationName, null, null);

      case "LocalJoin":
        /* Child 1 */
        childName = (String) jsonOperator.get("arg_child1");
        List<?> child1ColumnStr = (List<?>) jsonOperator.get("arg_columns1");
        /* Child 1 checks */
        Objects.requireNonNull(childName, "missing LocalJoin field: arg_child1");
        Objects.requireNonNull(child1ColumnStr, "missing LocalJoin field: arg_columns1");
        /* Child 2 arguments */
        child2Name = (String) jsonOperator.get("arg_child2");
        List<?> child2ColumnStr = (List<?>) jsonOperator.get("arg_columns2");
        /* Child 2 checks */
        Objects.requireNonNull(childName, "missing LocalJoin field: arg_child2");
        Objects.requireNonNull(child2ColumnStr, "missing LocalJoin field: arg_columns2");
        /* Mutual checks */
        Preconditions.checkState(child1ColumnStr.size() == child2ColumnStr.size(),
            "arg_columns1 and arg_columns2 must have the same length!");

        /* Convert the arguments over */
        child = operators.get(childName);
        Objects.requireNonNull(child, "LocalJoin child Operator " + childName + " not previously defined");
        child2 = operators.get(child2Name);
        Objects.requireNonNull(child2, "LocalJoin child2 Operator " + child2Name + " not previously defined");
        /* Child 1 columns */
        int[] child1Columns = new int[child1ColumnStr.size()];
        i = 0;
        for (final Object o : child1ColumnStr) {
          child1Columns[i] = Integer.parseInt((String) o);
        }
        /* Child 2 columns */
        int[] child2Columns = new int[child2ColumnStr.size()];
        i = 0;
        for (final Object o : child2ColumnStr) {
          child2Columns[i] = Integer.parseInt((String) o);
        }
        return new LocalJoin(child, child2, child1Columns, child2Columns);

      case "SQLiteScan":
        relationName = (String) jsonOperator.get("arg_relation_name");
        Objects.requireNonNull(relationName, "missing SQLiteScan field: arg_relation_name");
        Schema schema;
        try {
          schema = MasterApiServer.getMyriaServer().getSchema(relationName);
        } catch (final CatalogException e) {
          /* Throw a 500 (Internal Server Error) */
          throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).build());
        }
        if (schema == null) {
          throw new IOException("Specified relation " + relationName + " does not exist.");
        }
        return new SQLiteQueryScan(null, "SELECT * from " + relationName, schema);

      default:
        throw new RuntimeException("Not implemented deserializing Operator of type " + opType);
    }
  }
}
