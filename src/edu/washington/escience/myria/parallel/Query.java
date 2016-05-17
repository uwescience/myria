package edu.washington.escience.myria.parallel;

import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.encoding.QueryConstruct;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.util.ErrorUtils;

/**
 * Keeps track of all the state (statistics, subqueries, etc.) for a single Myria query.
 */
public final class Query {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Query.class);
  /** The id of this query. */
  private final long queryId;
  /** The id of the next subquery to be issued. */
  @GuardedBy("this")
  private long subqueryId;
  /** The status of the query. Must be kept synchronized. */
  @GuardedBy("this")
  private Status status;
  /** The subqueries to be executed. */
  @GuardedBy("this")
  private final LinkedList<SubQuery> subQueryQ;
  /** The query plan tasks to be executed. */
  @GuardedBy("this")
  private final LinkedList<QueryPlan> planQ;
  /** The execution statistics about this query. */
  @GuardedBy("this")
  private final ExecutionStatistics executionStats;
  /** The currently-executing subquery. */
  @GuardedBy("this")
  private SubQuery currentSubQuery;
  /** The server. */
  private final Server server;
  /** The message explaining why a query failed. */
  @GuardedBy("this")
  private String message;
  /** The future for this query. */
  private final QueryFuture future;
  /** True if the query should be run with profiling enabled. */
  private final Set<ProfilingMode> profiling;
  /** Indicates whether the query should be run with a particular fault tolerance mode. */
  private final FTMode ftMode;
  /** Global variables that are part of this query. */
  private final ConcurrentHashMap<String, Object> globals;
  /** Temporary relations created during the execution of this query. */
  private final ConcurrentHashMap<RelationKey, RelationWriteMetadata> tempRelations;
  /** resource usage stats of workers. */
  private final ConcurrentHashMap<Integer, ConcurrentLinkedDeque<ResourceStats>> resourceUsage;

  /**
   * Construct a new {@link Query} object for this query.
   *
   * @param queryId the id of this query
   * @param query contains the query options (profiling, fault tolerance)
   * @param plan the execution plan
   * @param server the server on which this query will be executed
   */
  public Query(
      final long queryId, final QueryEncoding query, final QueryPlan plan, final Server server) {
    Preconditions.checkNotNull(query, "query");
    this.server = Preconditions.checkNotNull(server, "server");
    profiling = ImmutableSet.copyOf(query.profilingMode);
    ftMode = query.ftMode;
    this.queryId = queryId;
    subqueryId = 0;
    synchronized (this) {
      status = Status.ACCEPTED;
    }
    executionStats = new ExecutionStatistics();
    subQueryQ = new LinkedList<>();
    planQ = new LinkedList<>();
    planQ.add(plan);
    message = null;
    future = QueryFuture.create(queryId);
    globals = new ConcurrentHashMap<>();
    tempRelations = new ConcurrentHashMap<>();
    resourceUsage = new ConcurrentHashMap<Integer, ConcurrentLinkedDeque<ResourceStats>>();
  }

  /**
   * Return the id of this query.
   *
   * @return the id of this query
   */
  public long getQueryId() {
    return queryId;
  }

  /**
   * Return the status of this query.
   *
   * @return the status of this query.
   */
  public synchronized QueryStatusEncoding.Status getStatus() {
    return status;
  }

  /**
   * Returns <code>true</code> if this query has finished, and <code>false</code> otherwise.
   *
   * @return <code>true</code> if this query has finished, and <code>false</code> otherwise
   */
  public synchronized boolean isDone() {
    return subQueryQ.isEmpty() && planQ.isEmpty();
  }

  /**
   * Returns the {@link SubQuery} that is currently executing, or <code>null</code> if nothing is running.
   *
   * @return the {@link SubQuery} that is currently executing, or <code>null</code> if nothing is running
   */
  public synchronized SubQuery getCurrentSubQuery() {
    return currentSubQuery;
  }

  /**
   * If the sub-query we're about to execute writes to any persistent relations, generate and enqueue the
   * "update tuple relation count" sub-query to be run next.
   *
   * @param subQuery the subquery about to be executed. This subquery must have already been removed from the queue.
   * @throws DbException if there is an error getting metadata about existing relations from the Server.
   */
  private synchronized void addDerivedSubQueries(final SubQuery subQuery) throws DbException {
    Map<RelationKey, RelationWriteMetadata> relationsWritten =
        currentSubQuery.getPersistentRelationWriteMetadata(server);
    if (!relationsWritten.isEmpty()) {
      SubQuery updateCatalog =
          QueryConstruct.getRelationTupleUpdateSubQuery(relationsWritten, server);
      subQueryQ.addFirst(updateCatalog);
    }
  }

  /**
   * Generates and returns the next {@link SubQuery} to run.
   *
   * @return the next {@link SubQuery} to run
   * @throws DbException if there is an error
   * @throws QueryKilledException if the query has been killed.
   */
  public synchronized SubQuery nextSubQuery() throws DbException, QueryKilledException {
    Preconditions.checkState(
        currentSubQuery == null, "must call finishSubQuery before calling nextSubQuery");
    if (isDone()) {
      return null;
    }
    if (status == Status.KILLING) {
      throw new QueryKilledException();
    }
    if (!subQueryQ.isEmpty()) {
      currentSubQuery = subQueryQ.removeFirst();
      SubQueryId sqId = new SubQueryId(queryId, subqueryId);
      currentSubQuery.setSubQueryId(sqId);
      addDerivedSubQueries(currentSubQuery);

      Set<ProfilingMode> profilingMode = getProfilingMode();
      if (!profilingMode.isEmpty()) {
        if (!currentSubQuery.isProfileable()) {
          profilingMode = ImmutableSet.of();
        } else {
          server.setQueryPlan(sqId, currentSubQuery.getEncodedPlan());
        }
      }

      QueryConstruct.setQueryExecutionOptions(
          currentSubQuery.getWorkerPlans(), ftMode, profilingMode);
      currentSubQuery.getMasterPlan().setFTMode(ftMode);
      currentSubQuery.getMasterPlan().setProfilingMode(ImmutableSet.<ProfilingMode>of());
      ++subqueryId;
      if (subqueryId >= MyriaConstants.MAXIMUM_NUM_SUBQUERIES) {
        throw new DbException(
            "Infinite-loop safeguard: quitting after "
                + MyriaConstants.MAXIMUM_NUM_SUBQUERIES
                + " subqueries.");
      }
      return currentSubQuery;
    }
    planQ.getFirst().instantiate(planQ, subQueryQ, new ConstructArgs(server, queryId));
    /*
     * The above line may have emptied planQ, mucked with subQueryQ, not sure. So just recurse to make sure we do the
     * right thing.
     */
    return nextSubQuery();
  }

  /**
   * Mark the current {@link SubQuery} as finished.
   */
  public synchronized void finishSubQuery() {
    currentSubQuery = null;
  }

  /**
   * Returns the time this query started, in ISO8601 format, or <code>null</code> if the query has not yet been started.
   *
   * @return the time this query started, in ISO8601 format, or <code>null</code> if the query has not yet been started
   */
  public synchronized DateTime getStartTime() {
    return executionStats.getStartTime();
  }

  /**
   * Set the time this query started to now in ISO8601 format.
   */
  public synchronized void markStart() {
    status = Status.RUNNING;
    executionStats.markStart();
  }

  /**
   * Set this query as having failed due to the specified cause.
   *
   * @param cause the reason the query failed.
   */
  public synchronized void markFailed(final Throwable cause) {
    Preconditions.checkNotNull(cause, "cause");
    if (Status.finished(status)) {
      LOGGER.warn(
          "Ignoring markFailed({}) because already finished: status {}, message {}",
          cause,
          status,
          message);
      return;
    }
    status = Status.ERROR;
    markEnd();
    message = ErrorUtils.getStackTrace(cause);
    future.setException(cause);
  }

  /**
   * Set the time this query ended to now in ISO8601 format.
   */
  public synchronized void markSuccess() {
    Verify.verify(currentSubQuery == null, "expect current subquery to be null when query ends");
    if (Status.finished(status)) {
      LOGGER.warn(
          "Ignoring markSuccess() because already finished: status {}, message {}",
          status,
          message);
      return;
    }
    status = Status.SUCCESS;
    markEnd();
    future.set(this);
  }

  /**
   * Set the time this query ended to now in ISO8601 format.
   */
  private synchronized void markEnd() {
    executionStats.markEnd();
  }

  /**
   * Returns the time this query ended, in ISO8601 format, or <code>null</code> if the query has not yet ended.
   *
   * @return the time this query ended, in ISO8601 format, or <code>null</code> if the query has not yet ended
   */
  public synchronized DateTime getEndTime() {
    return executionStats.getEndTime();
  }

  /**
   * Returns the time elapsed (in nanoseconds) since the query started.
   *
   * @return the time elapsed (in nanoseconds) since the query started
   */
  public synchronized Long getElapsedTime() {
    return executionStats.getQueryExecutionElapse();
  }

  /**
   * Return a message explaining why a query failed, or <code>null</code> if the query did not fail.
   *
   * @return a message explaining why a query failed, or <code>null</code> if the query did not fail
   */
  public synchronized String getMessage() {
    return message;
  }

  /**
   * Returns the future on the execution of this query.
   *
   * @return the future on the execution of this query
   */
  public QueryFuture getFuture() {
    return future;
  }

  /**
   * Call when the query has been killed.
   */
  public synchronized void markKilled() {
    markEnd();
    if (Status.finished(status)) {
      LOGGER.warn(
          "Ignoring markKilled() because already finished: status {}, message {}", status, message);
      return;
    }
    Preconditions.checkState(
        status == Status.KILLING,
        "cannot mark a query killed unless its status is KILLING, not %s",
        status);
    status = Status.KILLED;
    future.cancel(true);
  }

  /**
   * @return the fault tolerance mode for this query.
   */
  protected FTMode getFTMode() {
    return ftMode;
  }

  /**
   * @return true if this query should be profiled.
   */
  @Nonnull
  protected Set<ProfilingMode> getProfilingMode() {
    return profiling;
  }

  /**
   * Return the value of the global variable named by the specified key.
   *
   * @param key the name of the variable
   * @return the value of the variable, nor {@code null} if the variable does not exist.
   */
  public Object getGlobal(final String key) {
    return globals.get(key);
  }

  /**
   * Set the global variable named by the specified key to the specified value.
   *
   * @param key the name of the variable
   * @param value the new value for the variable
   */
  public void setGlobal(final String key, final Object value) {
    globals.put(key, value);
  }

  /**
   * Initiate the process of killing this query, if it's not done already.
   */
  public synchronized void kill() {
    if (!Status.ongoing(status)) {
      LOGGER.warn(
          "Ignoring kill() because query is not ongoing; status {}, message {}", status, message);
      return;
    }
    status = Status.KILLING;
    if (currentSubQuery != null) {
      server.getQueryManager().killSubQuery(currentSubQuery.getSubQueryId());
      currentSubQuery = null;
    }
  }

  /**
   * Determines and sanity-checks the set of relations created by the currently running subquery. Assuming the checks
   * pass, creates a {@link DatasetMetadataUpdater} future for this subquery and adds it as a pre-listener to the
   * specified {@code future}.
   *
   * @param catalog the Catalog in which the relation metadata will be updated
   * @param future the future on the subquery
   * @throws DbException if there is an error
   */
  public synchronized void addDatasetMetadataUpdater(
      final MasterCatalog catalog, final LocalSubQueryFuture future) throws DbException {
    SubQuery subQuery = currentSubQuery;
    final Map<RelationKey, RelationWriteMetadata> relationsCreated =
        subQuery.getRelationWriteMetadata(server);
    if (relationsCreated.size() == 0) {
      return;
    }

    /* Verify that the schemas for any temp relation we're not overwriting match the existing schema. */
    for (RelationWriteMetadata meta : relationsCreated.values()) {
      if (meta.isOverwrite()) {
        if (meta.isTemporary()) {
          tempRelations.put(meta.getRelationKey(), meta);
        }
        continue;
      }

      RelationKey relation = meta.getRelationKey();
      Schema oldSchema;
      String relationType;
      if (meta.isTemporary()) {
        relationType = "temporary";
        oldSchema = tempRelations.get(relation).getSchema();
      } else {
        relationType = "persistent";
        try {
          oldSchema = server.getSchema(relation);
        } catch (CatalogException e) {
          throw new DbException(
              Joiner.on(' ')
                  .join(
                      "Error checking catalog for schema of",
                      relation,
                      "during subquery",
                      subQuery.getSubQueryId()),
              e);
        }
      }
      if (oldSchema != null) {
        Preconditions.checkArgument(
            oldSchema.equals(meta.getSchema()),
            "Cannot append to existing %s relation %s (schema: %s) with new schema (%s)",
            relationType,
            relation,
            oldSchema,
            meta.getSchema());
      }
    }

    Map<RelationKey, RelationWriteMetadata> persistentRelations =
        subQuery.getPersistentRelationWriteMetadata(server);
    if (persistentRelations.size() == 0) {
      return;
    }
    /*
     * Add the DatasetMetadataUpdater, which will update the catalog with the set of workers created when the query
     * succeeds. Note that we only use persistent relations here.
     */
    DatasetMetadataUpdater dsmd =
        new DatasetMetadataUpdater(catalog, persistentRelations, subQuery.getSubQueryId());
    future.addPreListener(dsmd);
  }

  /**
   * Returns the schema for the specified temp relation.
   *
   * @param relationKey the desired temp relation
   * @return the schema for the specified temp relation
   */
  public Schema getTempSchema(@Nonnull final RelationKey relationKey) {
    return getMetadata(relationKey).getSchema();
  }

  /**
   * Returns the workers storing the specified temp relation.
   *
   * @param relationKey the desired temp relation
   * @return the set of workers storing the specified temp relation
   */
  public Set<Integer> getWorkersForTempRelation(@Nonnull final RelationKey relationKey) {
    return getMetadata(relationKey).getWorkers();
  }

  /**
   * Get the {@link RelationWriteMetadata} for the specified temp relation.
   *
   * @param relationKey the key of the temp relation
   * @return the {@link RelationWriteMetadata} for the specified temp relation
   * @throws IllegalArgumentException if there is no such temp relation
   */
  private RelationWriteMetadata getMetadata(@Nonnull final RelationKey relationKey) {
    Preconditions.checkNotNull(relationKey, "relationKey");
    RelationWriteMetadata meta = tempRelations.get(relationKey);
    Preconditions.checkArgument(
        meta != null, "Query #%s, no temp relation with key %s found", queryId, relationKey);
    return meta;
  }

  /**
   * @param senderId from whicht worker the stats were sent from
   * @param stats the stats
   */
  public void addResourceStats(final int senderId, final ResourceStats stats) {
    resourceUsage.putIfAbsent(senderId, new ConcurrentLinkedDeque<ResourceStats>());
    resourceUsage.get(senderId).add(stats);
  }

  /**
   * @return resource usage stats in a tuple buffer.
   */
  public TupleBuffer getResourceUsage() {
    Schema schema =
        Schema.appendColumn(MyriaConstants.RESOURCE_PROFILING_SCHEMA, Type.INT_TYPE, "workerId");
    TupleBuffer tb = new TupleBuffer(schema);
    for (int workerId : resourceUsage.keySet()) {
      ConcurrentLinkedDeque<ResourceStats> statsList = resourceUsage.get(workerId);
      for (ResourceStats stats : statsList) {
        tb.putLong(0, stats.getTimestamp());
        tb.putInt(1, stats.getOpId());
        tb.putString(2, stats.getMeasurement());
        tb.putLong(3, stats.getValue());
        tb.putLong(4, stats.getQueryId());
        tb.putLong(5, stats.getSubqueryId());
        tb.putInt(6, workerId);
      }
    }
    return tb;
  }
}
