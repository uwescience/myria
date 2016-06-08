package edu.washington.escience.myria.profiling;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.ResourceStats;
import edu.washington.escience.myria.parallel.SubQueryId;
import edu.washington.escience.myria.parallel.WorkerSubQuery;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * A logger for profiling data.
 */
public class ProfilingLogger {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ProfilingLogger.class);

  /** The connection to the database database. */
  private final JdbcAccessMethod accessMethod;

  /** Buffer for recorded events. */
  private final TupleBatchBuffer events;

  /** Buffer for tuples sent. */
  private final TupleBatchBuffer sent;

  /** Buffer for tuples sent. */
  private final TupleBatchBuffer resources;

  /**
   * Default constructor.
   *
   * @param connectionInfo connection information
   *
   * @throws DbException if any error occurs
   */
  public ProfilingLogger(final ConnectionInfo connectionInfo) throws DbException {
    Preconditions.checkArgument(
        connectionInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
        "Profiling only supported with Postgres JDBC connection");

    /* open the database connection */
    accessMethod =
        (JdbcAccessMethod) AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);

    accessMethod.createUnloggedTableIfNotExists(
        MyriaConstants.EVENT_PROFILING_RELATION, MyriaConstants.EVENT_PROFILING_SCHEMA);
    accessMethod.createTableIfNotExists(
        MyriaConstants.SENT_PROFILING_RELATION, MyriaConstants.SENT_PROFILING_SCHEMA);
    accessMethod.createUnloggedTableIfNotExists(
        MyriaConstants.RESOURCE_PROFILING_RELATION, MyriaConstants.RESOURCE_PROFILING_SCHEMA);

    createEventIndexes();
    createSentIndex();
    createResourceIndex();

    events = new TupleBatchBuffer(MyriaConstants.EVENT_PROFILING_SCHEMA);
    sent = new TupleBatchBuffer(MyriaConstants.SENT_PROFILING_SCHEMA);
    resources = new TupleBatchBuffer(MyriaConstants.RESOURCE_PROFILING_SCHEMA);
  }

  /**
   * @throws DbException if index cannot be created
   */
  protected void createSentIndex() throws DbException {
    final Schema schema = MyriaConstants.SENT_PROFILING_SCHEMA;

    List<IndexRef> index =
        ImmutableList.of(
            IndexRef.of(schema, "queryId"),
            IndexRef.of(schema, "subQueryId"),
            IndexRef.of(schema, "fragmentId"),
            IndexRef.of(schema, "destWorkerId"));
    try {
      accessMethod.createIndexIfNotExists(MyriaConstants.SENT_PROFILING_RELATION, schema, index);
    } catch (DbException e) {
      LOGGER.error("Couldn't create index for profiling logs:", e);
    }
  }

  /**
   * @throws DbException if index cannot be created
   */
  protected void createResourceIndex() throws DbException {
    final Schema schema = MyriaConstants.RESOURCE_PROFILING_SCHEMA;
    List<IndexRef> index =
        ImmutableList.of(
            IndexRef.of(schema, "queryId"),
            IndexRef.of(schema, "subqueryId"),
            IndexRef.of(schema, "opId"));
    try {
      accessMethod.createIndexIfNotExists(
          MyriaConstants.RESOURCE_PROFILING_RELATION, schema, index);
    } catch (DbException e) {
      LOGGER.error("Couldn't create index for profiling resource:", e);
    }
  }

  /**
   * @throws DbException if index cannot be created
   */
  protected void createEventIndexes() throws DbException {
    final Schema schema = MyriaConstants.EVENT_PROFILING_SCHEMA;

    List<IndexRef> rootOpsIndex =
        ImmutableList.of(
            IndexRef.of(schema, "queryId"),
            IndexRef.of(schema, "subQueryId"),
            IndexRef.of(schema, "fragmentId"),
            IndexRef.of(schema, "startTime"),
            IndexRef.of(schema, "endTime"));
    List<IndexRef> filterIndex =
        ImmutableList.of(
            IndexRef.of(schema, "queryId"),
            IndexRef.of(schema, "subQueryId"),
            IndexRef.of(schema, "fragmentId"),
            IndexRef.of(schema, "opId"),
            IndexRef.of(schema, "startTime"),
            IndexRef.of(schema, "endTime"));

    try {
      accessMethod.createIndexIfNotExists(
          MyriaConstants.EVENT_PROFILING_RELATION, schema, rootOpsIndex);
      accessMethod.createIndexIfNotExists(
          MyriaConstants.EVENT_PROFILING_RELATION, schema, filterIndex);
    } catch (DbException e) {
      LOGGER.error("Couldn't create index for profiling logs:", e);
    }
  }

  /**
   * Returns the relative time to the beginning on the query in nanoseconds.
   *
   * The time that we log is the relative time to the beginning of the query in nanoseconds (workerStartTimeMillis). We
   * assume that all workers get initialized at the same time so that the time in ms recorded on the workers can be
   * considered 0. The time in nanoseconds is only a relative time with regard to some arbitrary beginning and it is
   * different of different threads. This means we have to calculate the difference on the thread and add it to the
   * difference in ms between the worker and the thread (startupTimeMillis).
   *
   * @param operator the operator
   * @return the time to record
   */
  public long getTime(final Operator operator) {
    final WorkerSubQuery workerSubQuery = (WorkerSubQuery) operator.getLocalSubQuery();
    final long workerStartTimeMillis = workerSubQuery.getBeginMilliseconds();
    final long threadStartTimeMillis = operator.getFragment().getBeginMilliseconds();
    Preconditions.checkState(
        workerStartTimeMillis > 0, "Query initialization time has not been recorded.");
    Preconditions.checkState(threadStartTimeMillis > 0, "Thread time has not been recorded.");
    final long startupTimeMillis = threadStartTimeMillis - workerStartTimeMillis;
    Preconditions.checkState(
        startupTimeMillis >= 0,
        "Thread that works on fragment cannot run (%s) before query is initialized (%s).",
        workerStartTimeMillis,
        threadStartTimeMillis);
    final long threadStartNanos = operator.getFragment().getBeginNanoseconds();
    final long activeTimeNanos = System.nanoTime() - threadStartNanos;

    return TimeUnit.MILLISECONDS.toNanos(startupTimeMillis) + activeTimeNanos;
  }

  /**
   * Appends a single event appearing in an operator to a batch that is flushed either after a certain number of events
   * are in the batch or {@link #flushEventsBatch()} is called.
   *
   * @param operator the operator where this record was logged
   * @param numTuples the number of tuples
   * @param startTime the start time of the event in ns
   *
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordEvent(
      final Operator operator, final long numTuples, final long startTime) throws DbException {
    SubQueryId sq = operator.getSubQueryId();
    events.putLong(0, sq.getQueryId());
    events.putInt(1, (int) sq.getSubqueryId());
    events.putInt(2, operator.getFragmentId());
    events.putInt(3, Preconditions.checkNotNull(operator.getOpId(), "opId"));
    events.putLong(4, startTime);
    events.putLong(5, getTime(operator));
    events.putLong(6, numTuples);

    flush(MyriaConstants.EVENT_PROFILING_RELATION, events.popFilled());
  }

  /**
   * Record that data was sent to a worker. The buffer is flushed at a particular number of tuples or on a call to
   * {@link #flush()}.
   *
   * @param operator the operator where this record was logged
   * @param numTuples the number of tuples sent.
   * @param destWorkerId the worker if that we send the data to
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordSent(
      final Operator operator, final int numTuples, final int destWorkerId) throws DbException {
    SubQueryId sq = operator.getSubQueryId();
    sent.putLong(0, sq.getQueryId());
    sent.putInt(1, (int) sq.getSubqueryId());
    sent.putInt(2, operator.getFragmentId());
    sent.putLong(3, getTime(operator));
    sent.putLong(4, numTuples);
    sent.putInt(5, destWorkerId);

    flush(MyriaConstants.SENT_PROFILING_RELATION, sent.popFilled());
  }

  /**
   * Record a single resource stats. The buffer is flushed at a particular number of tuples or on a call to
   * {@link #flush()}.
   *
   * @param stats the resource stats.
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordResource(final ResourceStats stats) throws DbException {
    resources.putLong(0, stats.getTimestamp());
    resources.putInt(1, stats.getOpId());
    resources.putString(2, stats.getMeasurement());
    resources.putLong(3, stats.getValue());
    resources.putLong(4, stats.getQueryId());
    resources.putLong(5, stats.getSubqueryId());

    flush(MyriaConstants.RESOURCE_PROFILING_RELATION, resources.popFilled());
  }

  /**
   * Flush the profiling buffers. The buffer is flushed at a particular number of tuples or on a call to
   * {@link #flush()}.
   *
   * @throws DbException if insertion in the database fails
   */
  public synchronized void flush() throws DbException {
    flush(MyriaConstants.SENT_PROFILING_RELATION, sent.popAny());
    flush(MyriaConstants.EVENT_PROFILING_RELATION, events.popAny());
    flush(MyriaConstants.RESOURCE_PROFILING_RELATION, resources.popAny());

    Preconditions.checkState(sent.numTuples() == 0, "Unwritten sent profiling data.");
    Preconditions.checkState(events.numTuples() == 0, "Unwritten event profiling data.");
    Preconditions.checkState(resources.numTuples() == 0, "Unwritten resource profiling data.");
  }

  /**
   * Flush the tuple batch and log how long it took.
   *
   * @param relationKey the relation to write to
   * @param tupleBatch the tuples to write
   *
   * @throws DbException if insertion in the database fails
   */
  private void flush(final RelationKey relationKey, final TupleBatch tupleBatch)
      throws DbException {
    if (tupleBatch == null) {
      return;
    }

    final long startTime = System.nanoTime();

    accessMethod.tupleBatchInsert(relationKey, tupleBatch);

    LOGGER.info(
        "Writing profiling data to {} took {} milliseconds.",
        relationKey,
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
  }

  /**
   * Returns {@code true} if the current JDBC connection is active.
   *
   * @return {@code true} if the current JDBC connection is active.
   */
  public boolean isValid() {
    try {
      return accessMethod.getConnection().isValid(1);
    } catch (SQLException e) {
      LOGGER.warn("Error checking connection validity", e);
      return false;
    }
  }
}
