package edu.washington.escience.myria.parallel;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding.Status;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.proto.ControlProto;
import edu.washington.escience.myria.proto.ControlProto.ControlMessage;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.util.DateTimeUtils;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * This class manages the queries running on Myria. It encapsulates the state outside of the Server object and handles
 * concurrency issues w.r.t. issuing, enqueuing, advance queries.
 */
public class QueryManager {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(MasterCatalog.class);

  /**
   * Queries currently running (executing or being killed).
   */
  private final ConcurrentHashMap<Long, Query> runningQueries;

  /** The queries that are queued. */
  @GuardedBy("queryQueue")
  private final TreeMap<Long, Query> queryQueue;

  /**
   * Subqueries currently in execution.
   */
  private final ConcurrentHashMap<SubQueryId, MasterSubQuery> executingSubQueries;

  /** The Myria catalog. */
  private final MasterCatalog catalog;

  /** The Server on which this manager runs. */
  private final Server server;

  /**
   * This class encapsulates all the work of keeping track of queries. This includes dispatching query plans to workers,
   * starting the queries, killing queries, restarting queries, etc.
   *
   * @param catalog the master catalog. Gets updated when queries finish, for example.
   * @param server the server on which the queries are executed.
   */
  public QueryManager(final MasterCatalog catalog, final Server server) {
    this.catalog = catalog;
    this.server = server;
    queryQueue = Maps.newTreeMap();
    runningQueries = new ConcurrentHashMap<>();
    executingSubQueries = new ConcurrentHashMap<>();
  }

  /**
   * @return if a query is running.
   * @param queryId queryID.
   * @throws CatalogException if there is an exception checking the query status
   */
  public boolean queryCompleted(final long queryId) throws CatalogException {
    return Status.finished(getQueryStatus(queryId).status);
  }

  /**
   * update resource stats from messgaes.
   *
   * @param senderId the sender worer id.
   * @param m the message.
   */
  public void updateResourceStats(final int senderId, final ControlMessage m) {
    for (ControlProto.ResourceStats stats : m.getResourceStatsList()) {
      runningQueries
          .get(stats.getQueryId())
          .addResourceStats(senderId, ResourceStats.fromProtobuf(stats));
    }
  }

  /**
   * @return whether this master can handle more queries or not.
   */
  private boolean canSubmitQuery() {
    synchronized (queryQueue) {
      return ((runningQueries.size() + queryQueue.size()) < MyriaConstants.MAX_ACTIVE_QUERIES);
    }
  }

  /**
   * Finish the specified query by updating its status in the Catalog and then removing it from the active queries.
   *
   * @param queryState the query to be finished
   * @throws DbException if there is an error updating the Catalog
   */
  private void finishQuery(final Query queryState) throws DbException {
    LOGGER.info(
        "Finishing query {} with status {}", queryState.getQueryId(), queryState.getStatus());
    try {
      catalog.queryFinished(queryState);
    } catch (CatalogException e) {
      throw new DbException("Error finishing query " + queryState.getQueryId(), e);
    } finally {
      runningQueries.remove(queryState.getQueryId());

      /* See if we can submit a new query. */
      if (!runningQueries.isEmpty()) {
        /* TODO replace with proper scheduler as opposed to single-query-at-a-time. */
        return;
      }

      /* Now see if the query queue has anything for us. */
      Map.Entry<Long, Query> qEntry;
      synchronized (queryQueue) {
        qEntry = queryQueue.pollFirstEntry();
      }
      if (qEntry == null) {
        return;
      }
      Query q = qEntry.getValue();
      LOGGER.info("Now advancing to query {}", q.getQueryId());
      runningQueries.put(q.getQueryId(), q);
      advanceQuery(q);
    }
  }

  /**
   * Computes and returns the status of queries that have been submitted to Myria.
   *
   * @param limit the maximum number of results to return. Any value <= 0 is interpreted as all results.
   * @param maxId the largest query ID returned. If null or <= 0, all queries will be returned.
   * @param minId the smallest query ID returned. If null or <= 0, all queries will be returned. Ignored if maxId is
   *          present.
   * @param searchTerm a token to match against the raw queries. If null, all queries will be returned.
   * @throws CatalogException if there is an error in the catalog.
   * @return a list of the status of every query that has been submitted to Myria.
   */
  public List<QueryStatusEncoding> getQueries(
      @Nullable final Long limit,
      @Nullable final Long maxId,
      @Nullable final Long minId,
      @Nullable final String searchTerm)
      throws CatalogException {
    List<QueryStatusEncoding> ret = new LinkedList<>();

    /* Now add in the status for all the inactive (finished, killed, etc.) queries. */
    for (QueryStatusEncoding q : catalog.getQueries(limit, maxId, minId, searchTerm)) {
      if (QueryStatusEncoding.Status.finished(q.status)) {
        ret.add(q);
      } else {
        // If the query is not yet finished, refresh its status now.
        ret.add(getQueryStatus(q.queryId));
      }
    }

    return ret;
  }

  /**
   * Get the query with the specified ID, ensuring that it is active.
   *
   * @param queryId the id of the query to return.
   * @return the query with the specified ID, ensuring that it is active
   * @throws IllegalArgumentException if there is no active query with the given ID
   */
  @Nonnull
  public Query getQuery(@Nonnull final Long queryId) {
    Long qId = Preconditions.checkNotNull(queryId, "queryId");
    Query query;
    synchronized (queryQueue) {
      query = runningQueries.get(qId);
      if (query == null) {
        query = queryQueue.get(qId);
      }
    }
    Preconditions.checkArgument(query != null, "Query #%s is not active", queryId);
    return query;
  }

  /**
   * Computes and returns the status of the requested query, or null if the query does not exist.
   *
   * @param queryId the identifier of the query.
   * @throws CatalogException if there is an error in the catalog.
   * @return the status of this query.
   */
  public QueryStatusEncoding getQueryStatus(final long queryId) throws CatalogException {
    Query state = null;
    try {
      state = getQuery(queryId);
    } catch (IllegalArgumentException e) {
      ; /* Expected, if the query is not running. */
    }

    /* Get the stored data for this query, e.g., the submitted program. */
    QueryStatusEncoding queryStatus = catalog.getQuery(queryId);
    if (queryStatus == null) {
      return null;
    }

    if (state == null) {
      /* Not active, so the information from the Catalog is authoritative. */
      return queryStatus;
    }

    /* Currently active, so fill in the latest information about the query. */
    queryStatus.startTime = state.getStartTime();
    queryStatus.finishTime = state.getEndTime();
    queryStatus.elapsedNanos = state.getElapsedTime();
    queryStatus.status = state.getStatus();
    queryStatus.message = state.getMessage();
    return queryStatus;
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready. Returns null if there are too many active queries.
   *
   * @param queryId the catalog's assigned ID for this query.
   * @param query contains the query options (profiling, fault tolerance)
   * @param plan the query to be executed
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   */
  private QueryFuture submitQuery(
      final long queryId, final QueryEncoding query, final QueryPlan plan)
      throws DbException, CatalogException {
    final Query queryState = new Query(queryId, query, plan, server);
    boolean canStart = false;
    synchronized (queryQueue) {
      if (queryQueue.isEmpty() && runningQueries.isEmpty()) {
        canStart = true;
      } else {
        queryQueue.put(queryId, queryState);
      }
    }
    if (canStart) {
      runningQueries.put(queryId, queryState);
      advanceQuery(queryState);
    }
    return queryState.getFuture();
  }

  /**
   * @param queryId the query to be killed.
   */
  public void killQuery(final long queryId) {
    getQuery(queryId).kill();
  }

  /**
   * Find the master's subquery execution controller for the specified id.
   *
   * @param subQueryId the subquery id.
   * @return the master's subquery execution controller for the specified id.
   */
  @Nonnull
  private MasterSubQuery getMasterSubQuery(@Nonnull final SubQueryId subQueryId) {
    return Preconditions.checkNotNull(
        executingSubQueries.get(subQueryId),
        "MasterSubQuery for subquery {} not found",
        subQueryId);
  }

  /**
   * Inform the query manager that the specified worker has acknowledged receiving the specified subquery. Once all
   * workers have received the subquery, the subquery may be started.
   *
   * @param subQueryId the subquery.
   * @param workerId the worker.
   */
  public void workerReady(@Nonnull final SubQueryId subQueryId, final int workerId) {
    getMasterSubQuery(subQueryId).queryReceivedByWorker(workerId);
  }

  /**
   * Inform the query manager that the specified worker has successfully completed the specified subquery. Once all
   * workers have completed the subquery, the subquery is considered successful.
   *
   * @param subQueryId the subquery.
   * @param workerId the worker.
   */
  public void workerComplete(@Nonnull final SubQueryId subQueryId, final int workerId) {
    getMasterSubQuery(subQueryId).workerComplete(workerId);
  }

  /**
   * Inform the query manager that the specified worker has failed with the given cause.
   *
   * @param subQueryId the subquery.
   * @param workerId the worker.
   * @param cause the cause of the worker's failure.
   */
  public void workerFailed(
      @Nonnull final SubQueryId subQueryId, final int workerId, final Throwable cause) {
    getMasterSubQuery(subQueryId).workerFail(workerId, cause);
  }

  /**
   * Advance the given query to the next {@link SubQuery}. If there is no next {@link SubQuery}, mark the entire query
   * as having succeeded.
   *
   * @param queryState the specified query
   * @return the future of the next {@Link SubQuery}, or <code>null</code> if this query has succeeded.
   * @throws DbException if there is an error
   */
  private LocalSubQueryFuture advanceQuery(final Query queryState) throws DbException {
    Verify.verify(
        queryState.getCurrentSubQuery() == null, "expected queryState current task is null");

    SubQuery task;
    try {
      task = queryState.nextSubQuery();
    } catch (QueryKilledException qke) {
      queryState.markKilled();
      finishQuery(queryState);
      return null;
    } catch (RuntimeException | DbException e) {
      queryState.markFailed(e);
      finishQuery(queryState);
      return null;
    }
    if (task == null) {
      queryState.markSuccess();
      finishQuery(queryState);
      return null;
    }
    return submitSubQuery(queryState);
  }

  /**
   * Submit the next subquery in the query for execution, and return its future.
   *
   * @param queryState the query containing the subquery to be executed
   * @return the future of the subquery
   * @throws DbException if there is an error submitting the subquery for execution
   */
  private LocalSubQueryFuture submitSubQuery(final Query queryState) throws DbException {
    final SubQuery subQuery =
        Verify.verifyNotNull(
            queryState.getCurrentSubQuery(), "query state should have a current subquery");
    final SubQueryId subQueryId = subQuery.getSubQueryId();
    try {
      final MasterSubQuery mqp = new MasterSubQuery(subQuery, server);
      executingSubQueries.put(subQueryId, mqp);

      final LocalSubQueryFuture queryExecutionFuture = mqp.getExecutionFuture();

      /* Add the future to update the metadata about created relations, if there are any. */
      queryState.addDatasetMetadataUpdater(catalog, queryExecutionFuture);

      queryExecutionFuture.addListener(
          new LocalSubQueryFutureListener() {
            @Override
            public void operationComplete(final LocalSubQueryFuture future) throws Exception {

              finishSubQuery(subQueryId);

              final Long elapsedNanos = mqp.getExecutionStatistics().getQueryExecutionElapse();
              if (future.isSuccess()) {
                LOGGER.info(
                    "Subquery #{} succeeded. Time elapsed: {}.",
                    subQueryId,
                    DateTimeUtils.nanoElapseToHumanReadable(elapsedNanos));
                // TODO success management.
                advanceQuery(queryState);
              } else {
                Throwable cause = future.getCause();
                LOGGER.info(
                    "Subquery #{} failed. Time elapsed: {}. Failure cause is {}.",
                    subQueryId,
                    DateTimeUtils.nanoElapseToHumanReadable(elapsedNanos),
                    cause);
                if (cause instanceof QueryKilledException) {
                  queryState.markKilled();
                } else {
                  queryState.markFailed(cause);
                }
                finishQuery(queryState);
              }
            }
          });

      dispatchWorkerQueryPlans(mqp)
          .addListener(
              new LocalSubQueryFutureListener() {
                @Override
                public void operationComplete(final LocalSubQueryFuture future) throws Exception {
                  mqp.init();
                  if (subQueryId.getSubqueryId() == 0) {
                    getQuery(subQueryId.getQueryId()).markStart();
                  }
                  mqp.startExecution();
                  startWorkerQuery(future.getLocalSubQuery().getSubQueryId());
                }
              });

      return mqp.getExecutionFuture();
    } catch (DbException | RuntimeException e) {
      finishSubQuery(subQueryId);
      queryState.markFailed(e);
      finishQuery(queryState);
      throw e;
    }
  }

  /**
   * @param queryId the query id to fetch
   * @return resource usage stats in a tuple buffer or null if the query is not running.
   */
  public TupleBuffer getResourceUsage(final long queryId) {
    Query q = runningQueries.get(queryId);
    if (q == null) {
      return null;
    }
    return q.getResourceUsage();
  }

  /**
   * @param mqp the master query
   * @return the query dispatch {@link LocalSubQueryFuture}.
   * @throws DbException if any error occurs.
   */
  private LocalSubQueryFuture dispatchWorkerQueryPlans(final MasterSubQuery mqp)
      throws DbException {
    if (!server.getAliveWorkers().containsAll(mqp.getWorkerPlans().keySet())) {
      throw new DbException("Not all requested workers are alive.");
    }
    // directly set the master part as already received.
    mqp.queryReceivedByWorker(MyriaConstants.MASTER_ID);
    for (final Map.Entry<Integer, SubQueryPlan> e : mqp.getWorkerPlans().entrySet()) {
      int workerId = e.getKey();
      try {
        server
            .getIPCConnectionPool()
            .sendShortMessage(workerId, IPCUtils.queryMessage(mqp.getSubQueryId(), e.getValue()));
      } catch (final IOException ee) {
        throw new DbException(ee);
      }
    }
    return mqp.getWorkerReceiveFuture();
  }

  /**
   * Tells all the workers to begin executing the specified {@link SubQuery}.
   *
   * @param subQueryId the id of the subquery to be started.
   */
  private void startWorkerQuery(final SubQueryId subQueryId) {
    final MasterSubQuery mqp = executingSubQueries.get(subQueryId);
    for (final Integer workerID : mqp.getWorkerAssigned()) {
      server.getIPCConnectionPool().sendShortMessage(workerID, IPCUtils.startQueryTM(subQueryId));
    }
  }

  /**
   * Finish the subquery by removing it from the data structures.
   *
   * @param subQueryId the id of the subquery to finish.
   */
  private void finishSubQuery(final SubQueryId subQueryId) {
    LOGGER.info("Finishing subquery {}", subQueryId);
    long queryId = subQueryId.getQueryId();
    executingSubQueries.remove(subQueryId);
    getQuery(queryId).finishSubQuery();
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready. Returns null if there are too many active queries.
   *
   * @param query the query encoding.
   * @param plan the query to be executed
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   */
  public QueryFuture submitQuery(final QueryEncoding query, final QueryPlan plan)
      throws DbException, CatalogException {
    if (!canSubmitQuery()) {
      throw new DbException("Cannot submit query");
    }
    if (!query.profilingMode.isEmpty()) {
      if (!server.getDBMS().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
        throw new DbException(
            "Profiling mode is only supported when using Postgres as the storage system.");
      }
    }
    final long queryID = catalog.newQuery(query);
    return submitQuery(queryID, query, plan);
  }

  /**
   * @param queryId the query that owns the desired temp relation.
   * @param relationKey the key of the desired temp relation.
   * @return the list of workers that store the specified relation.
   */
  public Set<Integer> getWorkersForTempRelation(
      @Nonnull final Long queryId, @Nonnull final RelationKey relationKey) {
    return getQuery(queryId).getWorkersForTempRelation(relationKey);
  }

  /**
   * Kill a subquery.
   *
   * @param subQueryId the ID of the subquery to be killed
   */
  protected void killSubQuery(final SubQueryId subQueryId) {
    Preconditions.checkNotNull(subQueryId, "subQueryId");
    MasterSubQuery subQuery = executingSubQueries.get(subQueryId);
    if (subQuery != null) {
      subQuery.kill();
    } else {
      LOGGER.warn("tried to kill subquery {} but it is not executing.", subQueryId);
    }
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready. Returns null if there are too many active queries.
   *
   * @param rawQuery the raw user-defined query. E.g., the source Datalog program.
   * @param logicalRa the logical relational algebra of the compiled plan.
   * @param physicalPlan the Myria physical plan for the query.
   * @param workerPlans the physical parallel plan fragments for each worker.
   * @param masterPlan the physical parallel plan fragment for the master.
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   */
  public QueryFuture submitQuery(
      final String rawQuery,
      final String logicalRa,
      final String physicalPlan,
      final SubQueryPlan masterPlan,
      final Map<Integer, SubQueryPlan> workerPlans)
      throws DbException, CatalogException {
    return submitQuery(rawQuery, logicalRa, physicalPlan, new SubQuery(masterPlan, workerPlans));
  }

  /**
   * Submit a query for execution. The workerPlans may be removed in the future if the query compiler and schedulers are
   * ready. Returns null if there are too many active queries.
   *
   * @param rawQuery the raw user-defined query. E.g., the source Datalog program.
   * @param logicalRa the logical relational algebra of the compiled plan.
   * @param physicalPlan the Myria physical plan for the query.
   * @param plan the query plan.
   * @throws DbException if any error in non-catalog data processing
   * @throws CatalogException if any error in processing catalog
   * @return the query future from which the query status can be looked up.
   */
  public QueryFuture submitQuery(
      final String rawQuery,
      final String logicalRa,
      final String physicalPlan,
      final QueryPlan plan)
      throws DbException, CatalogException {
    QueryEncoding query = new QueryEncoding();
    query.rawQuery = rawQuery;
    query.logicalRa = rawQuery;
    query.fragments = ImmutableList.of();
    return submitQuery(query, plan);
  }

  /**
   * Kill all queries currently executing.
   */
  protected void killAll() {
    synchronized (queryQueue) {
      for (Query q : queryQueue.values()) {
        q.kill();
      }
      queryQueue.clear();
    }
    for (MasterSubQuery p : executingSubQueries.values()) {
      p.kill();
    }
  }

  /**
   * Inform the query manager that the specified worker has died. Depending on their fault-tolerance mode, executing
   * queries may be killed or be told that the worker has died.
   *
   * @param workerId the worker that died.
   */
  protected void workerDied(final int workerId) {
    for (MasterSubQuery mqp : executingSubQueries.values()) {
      /* for each alive query that the failed worker is assigned to, tell the query that the worker failed. */
      if (mqp.getWorkerAssigned().contains(workerId)) {
        mqp.workerFail(workerId, new LostHeartbeatException());
      }

      if (mqp.getFTMode().equals(FTMode.ABANDON)) {
        mqp.getMissingWorkers().add(workerId);
        mqp.updateProducerChannels(workerId, false);
        mqp.triggerFragmentEosEoiChecks();
      } else if (mqp.getFTMode().equals(FTMode.REJOIN)) {
        mqp.getMissingWorkers().add(workerId);
        mqp.updateProducerChannels(workerId, false);
      }
    }
  }

  /**
   * Inform the query manager that the specified worker has restarted. Executing queries in REJOIN mode may be informed
   * that the worker is alive, assuming that all workers have acknowledged its rebirth.
   *
   * @param workerId the worker that has restarted.
   * @param workersAcked the workers that have acknowledged its rebirth.
   */
  protected void workerRestarted(final int workerId, final Set<Integer> workersAcked) {
    for (MasterSubQuery mqp : executingSubQueries.values()) {
      if (mqp.getFTMode().equals(FTMode.REJOIN)
          && mqp.getMissingWorkers().contains(workerId)
          && workersAcked.containsAll(mqp.getWorkerAssigned())) {
        /* so a following ADD_WORKER_ACK won't cause queryMessage to be sent again */
        mqp.getMissingWorkers().remove(workerId);
        try {
          server
              .getIPCConnectionPool()
              .sendShortMessage(
                  workerId,
                  IPCUtils.queryMessage(mqp.getSubQueryId(), mqp.getWorkerPlans().get(workerId)));
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
