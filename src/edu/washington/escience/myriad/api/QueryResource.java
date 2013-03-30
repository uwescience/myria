package edu.washington.escience.myriad.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.coordinator.catalog.CatalogException;
import edu.washington.escience.myriad.operator.DupElim;
import edu.washington.escience.myriad.operator.IDBInput;
import edu.washington.escience.myriad.operator.LocalJoin;
import edu.washington.escience.myriad.operator.Merge;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.Project;
import edu.washington.escience.myriad.operator.SQLiteInsert;
import edu.washington.escience.myriad.operator.SQLiteQueryScan;
import edu.washington.escience.myriad.parallel.CollectConsumer;
import edu.washington.escience.myriad.parallel.CollectProducer;
import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.EOSController;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;
import edu.washington.escience.myriad.parallel.LocalMultiwayConsumer;
import edu.washington.escience.myriad.parallel.LocalMultiwayProducer;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.parallel.RoundRobinPartitionFunction;
import edu.washington.escience.myriad.parallel.ShuffleConsumer;
import edu.washington.escience.myriad.parallel.ShuffleProducer;
import edu.washington.escience.myriad.parallel.SingleFieldHashPartitionFunction;

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
   * @param userData the payload of the POST request itself.
   * @param uriInfo the URI of the current request.
   * @return the URI of the created query.
   */
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response postNewQuery(final Map<?, ?> userData, @Context final UriInfo uriInfo) {
    try {
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
      final String expectedResultSize = (String) userData.get("expected_result_size");
      Map<Integer, Operator[]> queryPlan = deserializeJsonQueryPlan(userData.get("query_plan"));

      Set<Integer> usingWorkers = new HashSet<Integer>();
      usingWorkers.addAll(queryPlan.keySet());
      /* Remove the server plan if present */
      usingWorkers.remove(0);
      /* Make sure that the requested workers are alive. */
      if (!MyriaApiUtils.getServer().getAliveWorkers().containsAll(usingWorkers)) {
        /* Throw a 503 (Service Unavailable) */
        throw new WebApplicationException(Response.status(Status.SERVICE_UNAVAILABLE).build());
      }

      /* Start the query, and get its Server-assigned Query ID */
      final Long queryId =
          MyriaApiUtils.getServer().startQuery(rawQuery, logicalRa, queryPlan, expectedResultSize);
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

  private static Operator[] deserializeJsonLocalPlans(final Object jsonLocalPlanList) throws Exception {
    /* Better be a List */
    if (!(jsonLocalPlanList instanceof List)) {
      throw new ClassCastException("argument is not a List of Operator definitions.");
    }
    List<?> localPlanList = (List<?>) jsonLocalPlanList;
    Operator[] ret = new Operator[localPlanList.size()];
    int i = 0;
    for (Object o : localPlanList) {
      ret[i++] = deserializeJsonLocalPlan(o);
    }
    return ret;
  }

  private static Operator deserializeJsonLocalPlan(final Object jsonLocalPlan) throws Exception {
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
    String userName;
    String programName;
    String relationName;

    /* Do the case-by-case work. */
    switch (opType) {

      case "SQLiteInsert": {
        String childName = deserializeString(jsonOperator, "arg_child");
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "SQLiteInsert child Operator " + childName + " not previously defined");
        userName = deserializeString(jsonOperator, "arg_user_name");
        programName = deserializeString(jsonOperator, "arg_program_name");
        relationName = deserializeString(jsonOperator, "arg_relation_name");
        String overwriteString = deserializeOptionalField(jsonOperator, "arg_overwrite_table");
        Boolean overwrite = Boolean.FALSE;
        if (overwriteString != null) {
          overwrite = Boolean.parseBoolean(overwriteString);
        }
        return new SQLiteInsert(child, RelationKey.of(userName, programName, relationName), null, null, overwrite);
      }

      case "LocalJoin": {
        /* Child 1 */
        String childName = deserializeString(jsonOperator, "arg_child1");
        int[] child1columns = deserializeIntArray(jsonOperator, "arg_columns1", false);
        /* Child 2 arguments */
        String child2Name = deserializeString(jsonOperator, "arg_child2");
        int[] child2columns = deserializeIntArray(jsonOperator, "arg_columns2", false);
        /* Mutual checks */
        Preconditions.checkState(child1columns.length == child2columns.length,
            "arg_columns1 and arg_columns2 must have the same length!");

        /* Find the operators. */
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "LocalJoin child Operator " + childName + " not previously defined");
        Operator child2 = operators.get(child2Name);
        Objects.requireNonNull(child2, "LocalJoin child2 Operator " + child2Name + " not previously defined");

        /* Get the optional arguments. */
        int[] child1select = deserializeIntArray(jsonOperator, "arg_select1", true);
        int[] child2select = deserializeIntArray(jsonOperator, "arg_select2", true);
        if ((child1select != null) && (child2select != null)) {
          return new LocalJoin(child, child2, child1columns, child2columns, child1select, child2select);
        }
        if ((child1select == null) && (child2select == null)) {
          return new LocalJoin(child, child2, child1columns, child2columns);
        }
        throw new IllegalArgumentException(
            "LocalJoin: either both or neither of arg_select1 and arg_select2 must be specified");
      }

      case "SQLiteScan": {
        userName = deserializeString(jsonOperator, "arg_user_name");
        programName = deserializeString(jsonOperator, "arg_program_name");
        relationName = deserializeString(jsonOperator, "arg_relation_name");
        RelationKey relationKey = RelationKey.of(userName, programName, relationName);
        Schema schema;
        try {
          schema = MyriaApiUtils.getServer().getSchema(relationKey);
        } catch (final CatalogException e) {
          /* Throw a 500 (Internal Server Error) */
          throw new WebApplicationException(Response.status(Status.INTERNAL_SERVER_ERROR).build());
        }
        if (schema == null) {
          throw new IOException("Specified relation " + relationKey.toString("sqlite") + " does not exist.");
        }
        return new SQLiteQueryScan(null, "SELECT * from " + relationKey.toString("sqlite"), schema);
      }

      case "Consumer": {
        Schema schema = deserializeSchema(jsonOperator, "arg_schema");
        int[] workerIDs = deserializeIntArray(jsonOperator, "arg_workerIDs", false);
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        return new Consumer(schema, operatorID, workerIDs);
      }

      case "ShuffleConsumer": {
        Schema schema = deserializeSchema(jsonOperator, "arg_schema");
        int[] workerIDs = deserializeIntArray(jsonOperator, "arg_workerIDs", false);
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        return new ShuffleConsumer(schema, operatorID, workerIDs);
      }

      case "CollectConsumer": {
        Schema schema = deserializeSchema(jsonOperator, "arg_schema");
        int[] workerIDs = deserializeIntArray(jsonOperator, "arg_workerIDs", false);
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        return new CollectConsumer(schema, operatorID, workerIDs);
      }

      case "LocalMultiwayConsumer": {
        Schema schema = deserializeSchema(jsonOperator, "arg_schema");
        int workerID = deserializeInt(jsonOperator, "arg_workerID");
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        return new LocalMultiwayConsumer(schema, operatorID, workerID);
      }

      case "ShuffleProducer": {
        int[] workerIDs = deserializeIntArray(jsonOperator, "arg_workerIDs", false);
        PartitionFunction<?, ?> pf = deserializePF(jsonOperator, "arg_pf", workerIDs.length);
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        String childName = deserializeString(jsonOperator, "arg_child");
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "ShuffleProducer child Operator " + childName + " not previously defined");
        return new ShuffleProducer(child, operatorID, workerIDs, pf);
      }

      case "CollectProducer": {
        int workerID = deserializeInt(jsonOperator, "arg_workerID");
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        String childName = deserializeString(jsonOperator, "arg_child");
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "CollectProducer child Operator " + childName + " not previously defined");
        return new CollectProducer(child, operatorID, workerID);
      }

      case "LocalMultiwayProducer": {
        int workerID = deserializeInt(jsonOperator, "arg_workerID");
        long[] tmpOpIDs = deserializeLongArray(jsonOperator, "arg_operatorIDs", false);
        ExchangePairID[] operatorIDs = new ExchangePairID[tmpOpIDs.length];
        for (int i = 0; i < tmpOpIDs.length; ++i) {
          operatorIDs[i] = ExchangePairID.fromExisting(tmpOpIDs[i]);
        }
        String childName = deserializeString(jsonOperator, "arg_child");
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "CollectProducer child Operator " + childName + " not previously defined");
        return new LocalMultiwayProducer(child, operatorIDs, workerID);
      }

      case "IDBInput": {
        Schema schema = deserializeSchema(jsonOperator, "arg_schema");
        int selfWorkerID = deserializeInt(jsonOperator, "arg_workerID");
        int selfIDBID = deserializeInt(jsonOperator, "arg_idbID");
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        int controllerWorkerID = deserializeInt(jsonOperator, "arg_controllerWorkerID");
        String child1Name = deserializeString(jsonOperator, "arg_child1");
        String child2Name = deserializeString(jsonOperator, "arg_child2");
        String child3Name = deserializeString(jsonOperator, "arg_child3");
        Operator child1 = operators.get(child1Name);
        Operator child2 = operators.get(child2Name);
        Operator child3 = operators.get(child3Name);
        Objects.requireNonNull(child1, "IDBInput child1 Operator " + child1Name + " not previously defined");
        Objects.requireNonNull(child2, "IDBInput child2 Operator " + child2Name + " not previously defined");
        Objects.requireNonNull(child3, "IDBInput child3 Operator " + child3Name + " not previously defined");
        return new IDBInput(schema, selfWorkerID, selfIDBID, operatorID, controllerWorkerID, child1, child2, child3);
      }

      case "EOSController": {
        String childName = deserializeString(jsonOperator, "arg_child");
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "IDBInput child Operator " + childName + " not previously defined");
        ExchangePairID operatorID = ExchangePairID.fromExisting(deserializeLong(jsonOperator, "arg_operatorID"));
        long[] tmpOpIDs = deserializeLongArray(jsonOperator, "arg_idbOpIDs", false);
        ExchangePairID[] idbOpIDs = new ExchangePairID[tmpOpIDs.length];
        for (int i = 0; i < tmpOpIDs.length; ++i) {
          idbOpIDs[i] = ExchangePairID.fromExisting(tmpOpIDs[i]);
        }
        int[] workerIDs = deserializeIntArray(jsonOperator, "arg_workerIDs", false);
        return new EOSController(child, operatorID, idbOpIDs, workerIDs);
      }

      case "DupElim": {
        String childName = deserializeString(jsonOperator, "arg_child");
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "DupElim child Operator " + childName + " not previously defined");
        return new DupElim(child);
      }

      case "Merge": {
        Schema schema = deserializeSchema(jsonOperator, "arg_schema");
        String child1Name = deserializeString(jsonOperator, "arg_child1");
        Operator child1 = operators.get(child1Name);
        Objects.requireNonNull(child1, "Merge child1 Operator " + child1Name + " not previously defined");
        String child2Name = deserializeString(jsonOperator, "arg_child2");
        Operator child2 = operators.get(child2Name);
        Objects.requireNonNull(child2, "Merge child2 Operator " + child2Name + " not previously defined");
        return new Merge(schema, child1, child2);
      }

      case "Project": {
        int[] fieldList = deserializeIntArray(jsonOperator, "arg_fieldList", false);
        String childName = deserializeString(jsonOperator, "arg_child");
        Operator child = operators.get(childName);
        Objects.requireNonNull(child, "Merge child Operator " + childName + " not previously defined");
        return new Project(fieldList, child);
      }

      default:
        throw new RuntimeException("Not implemented deserializing Operator of type " + opType);
    }
  }

  /**
   * Helper function to deserialize a String.
   * 
   * @param map the JSON map.
   * @param field the name of the field.
   * @return the String value of the field.
   * @throws NullPointerException if the field is not present.
   */
  private static String deserializeString(final Map<String, Object> map, final String field) {
    Object ret = map.get(field);
    Objects.requireNonNull(ret, "missing field: " + field);
    return (String) ret;
  }

  /**
   * Helper function to deserialize an optional String.
   * 
   * @param map the JSON map.
   * @param field the name of the field.
   * @return the String value of the field, or null if the field is not present.
   */
  private static String deserializeOptionalField(final Map<String, Object> map, final String field) {
    Object ret = map.get(field);
    return (String) ret;
  }

  /**
   * Helper function to deserialize an integer.
   * 
   * @param map the JSON map.
   * @param field the field containing the list.
   * @return the integer, or null if the field is missing and optional is true.
   */
  private static int deserializeInt(final Map<String, Object> map, final String field) {
    return Integer.parseInt(deserializeString(map, field));
  }

  /**
   * Helper function to deserialize an long.
   * 
   * @param map the JSON map.
   * @param field the field containing the list.
   * @return the long, or null if the field is missing and optional is true.
   */
  private static long deserializeLong(final Map<String, Object> map, final String field) {
    return Long.parseLong(deserializeString(map, field));
  }

  /**
   * Helper function to deserialize an array of Integers.
   * 
   * @param map the JSON map.
   * @param field the field containing the list.
   * @param optional whether the field is optional, or an IllegalArgumentException should be thrown.
   * @return the list of integers stored in field, or null if the field is missing and optional is true.
   */
  private static String[] deserializeStringArray(final Map<String, Object> map, final String field,
      final boolean optional) {
    List<?> list = (List<?>) map.get(field);
    if (list == null) {
      if (optional) {
        return null;
      }
      Preconditions.checkArgument(false, "mandatory field " + field + " missing");
    }
    String[] ret = new String[list.size()];
    for (int i = 0; i < ret.length; ++i) {
      ret[i] = (String) list.get(i);
    }
    return ret;
  }

  /**
   * Helper function to deserialize an array of Integers.
   * 
   * @param map the JSON map.
   * @param field the field containing the list.
   * @param optional whether the field is optional, or an IllegalArgumentException should be thrown.
   * @return the list of integers stored in field, or null if the field is missing and optional is true.
   */
  private static int[] deserializeIntArray(final Map<String, Object> map, final String field, final boolean optional) {
    String[] tmp = deserializeStringArray(map, field, optional);
    int[] ret = new int[tmp.length];
    for (int i = 0; i < tmp.length; ++i) {
      ret[i] = Integer.parseInt(tmp[i]);
    }
    return ret;
  }

  /**
   * Helper function to deserialize an array of Longs.
   * 
   * @param map the JSON map.
   * @param field the field containing the list.
   * @param optional whether the field is optional, or an IllegalArgumentException should be thrown.
   * @return the list of longs stored in field, or null if the field is missing and optional is true.
   */
  private static long[] deserializeLongArray(final Map<String, Object> map, final String field, final boolean optional) {
    String[] tmp = deserializeStringArray(map, field, optional);
    long[] ret = new long[tmp.length];
    for (int i = 0; i < tmp.length; ++i) {
      ret[i] = Long.parseLong(tmp[i]);
    }
    return ret;
  }

  /**
   * Helper function to deserialize a schema.
   * 
   * @param map the JSON map.
   * @param field the field containing the list.
   * @return the schema, or null if the field is missing and optional is true.
   */
  private static Schema deserializeSchema(final Map<String, Object> map, final String field) {
    Schema schema;
    LinkedHashMap<?, ?> tmp = (LinkedHashMap<?, ?>) map.get(field);
    @SuppressWarnings("unchecked")
    List<String> tmpTypes = (List<String>) tmp.get("column_types");
    List<Type> types = new ArrayList<Type>();

    for (String s : tmpTypes) {
      switch (s) {
        case "INT_TYPE":
          types.add(Type.INT_TYPE);
          break;
        case "FLOAT_TYPE":
          types.add(Type.FLOAT_TYPE);
          break;
        case "DOUBLE_TYPE":
          types.add(Type.DOUBLE_TYPE);
          break;
        case "BOOLEAN_TYPE":
          types.add(Type.BOOLEAN_TYPE);
          break;
        case "STRING_TYPE":
          types.add(Type.STRING_TYPE);
          break;
        case "LONG_TYPE":
          types.add(Type.LONG_TYPE);
          break;
      }
    }
    @SuppressWarnings("unchecked")
    List<String> names = (List<String>) tmp.get("column_names");
    schema = Schema.of(types, names);
    // schema = objectMapper.readValue((String) (map.get(field)), Schema.class);
    if (schema == null) {
      Preconditions.checkArgument(false, "mandatory field " + field + " missing");
    }
    return schema;
  }

  /**
   * Helper function to deserialize a partition function.
   * 
   * @param map the JSON map.
   * @param field the field containing the list.
   * @return the schema, or null if the field is missing and optional is true.
   */
  private static PartitionFunction<?, ?> deserializePF(final Map<String, Object> map, final String field,
      final int numWorker) {
    List<?> list = (List<?>) map.get(field);
    if (list == null) {
      Preconditions.checkArgument(false, "mandatory field " + field + " missing");
    }
    if (list.size() == 1) {
      Preconditions.checkArgument(((String) list.get(0)).equals("RoundRobin"), "unknown partition function "
          + (String) list.get(0));
      return new RoundRobinPartitionFunction(numWorker);
    } else if (list.size() == 2) {
      Preconditions.checkArgument(((String) list.get(0)).equals("SingleFieldHash"), "unknown partition function "
          + (String) list.get(0));
      int tmp = Integer.parseInt((String) list.get(1));
      SingleFieldHashPartitionFunction pf = new SingleFieldHashPartitionFunction(numWorker);
      pf.setAttribute(SingleFieldHashPartitionFunction.FIELD_INDEX, tmp);
      return pf;
    } else {
      Preconditions.checkArgument(false, "unknown partition function " + list);
    }
    return null;
  }
}
