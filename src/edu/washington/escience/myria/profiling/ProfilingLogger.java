package edu.washington.escience.myria.profiling;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.AccessMethod.IndexRef;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.ResourceStats;
import edu.washington.escience.myria.parallel.WorkerSubQuery;

/**
 * A logger for profiling data.
 */
public class ProfilingLogger {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ProfilingLogger.class);

  /** The connection to the database database. */
  private final JdbcAccessMethod accessMethod;

  /** The jdbc connection. */
  private final Connection connection;

  /** A query statement for batching. */
  private PreparedStatement statementEvent;

  /** A query statement for batching. */
  private PreparedStatement statementSent;

  /** A query statement for batching. */
  private PreparedStatement statementResource;

  /** Number of rows in batch in {@link #statementEvent}. */
  private int batchSizeEvents = 0;

  /** Number of rows in batch in {@link #statementSent}. */
  private int batchSizeSent = 0;

  /** Number of rows in batch in {@link #statementResource}. */
  private int batchSizeResource = 0;

  /**
   * Default constructor.
   * 
   * @param connectionInfo connection information
   * 
   * @throws DbException if any error occurs
   */
  public ProfilingLogger(final ConnectionInfo connectionInfo) throws DbException {
    Preconditions.checkArgument(connectionInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
        "Profiling only supported with Postgres JDBC connection");

    /* open the database connection */
    accessMethod = (JdbcAccessMethod) AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);

    accessMethod.createUnloggedTableIfNotExists(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA);
    accessMethod.createTableIfNotExists(MyriaConstants.SENT_RELATION, MyriaConstants.SENT_SCHEMA);
    accessMethod.createUnloggedTableIfNotExists(MyriaConstants.RESOURCE_RELATION, MyriaConstants.RESOURCE_SCHEMA);

    createProfilingIndexes();
    createSentIndex();
    createResourceIndex();

    connection = accessMethod.getConnection();
    try {
      statementEvent =
          connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.PROFILING_SCHEMA,
              MyriaConstants.PROFILING_RELATION));
      statementSent =
          connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.SENT_SCHEMA,
              MyriaConstants.SENT_RELATION));
      statementResource =
          connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.RESOURCE_SCHEMA,
              MyriaConstants.RESOURCE_RELATION));
    } catch (SQLException e) {
      throw new DbException(e);
    }
  }

  /**
   * @throws DbException if index cannot be created
   */
  protected void createSentIndex() throws DbException {
    final Schema schema = MyriaConstants.SENT_SCHEMA;

    List<IndexRef> index =
        ImmutableList.of(IndexRef.of(schema, "queryId"), IndexRef.of(schema, "fragmentId"), IndexRef.of(schema,
            "nanoTime"));
    try {
      accessMethod.createIndexIfNotExists(MyriaConstants.SENT_RELATION, schema, index);
    } catch (DbException e) {
      LOGGER.error("Couldn't create index for profiling logs:", e);
    }
  }

  /**
   * @throws DbException if index cannot be created
   */
  protected void createResourceIndex() throws DbException {
    final Schema schema = MyriaConstants.RESOURCE_SCHEMA;
    List<IndexRef> index =
        ImmutableList
            .of(IndexRef.of(schema, "queryId"), IndexRef.of(schema, "subqueryId"), IndexRef.of(schema, "opId"));
    try {
      accessMethod.createIndexIfNotExists(MyriaConstants.RESOURCE_RELATION, schema, index);
    } catch (DbException e) {
      LOGGER.error("Couldn't create index for profiling resource:", e);
    }
  }

  /**
   * @throws DbException if index cannot be created
   */
  protected void createProfilingIndexes() throws DbException {
    final Schema schema = MyriaConstants.PROFILING_SCHEMA;

    List<IndexRef> rootOpsIndex =
        ImmutableList.of(IndexRef.of(schema, "queryId"), IndexRef.of(schema, "fragmentId"), IndexRef.of(schema,
            "startTime"), IndexRef.of(schema, "endTime"));
    List<IndexRef> filterIndex =
        ImmutableList.of(IndexRef.of(schema, "queryId"), IndexRef.of(schema, "fragmentId"),
            IndexRef.of(schema, "opId"), IndexRef.of(schema, "startTime"), IndexRef.of(schema, "endTime"));

    try {
      accessMethod.createIndexIfNotExists(MyriaConstants.PROFILING_RELATION, schema, rootOpsIndex);
      accessMethod.createIndexIfNotExists(MyriaConstants.PROFILING_RELATION, schema, filterIndex);
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
    Preconditions.checkState(workerStartTimeMillis > 0, "Query initialization time has not been recorded.");
    Preconditions.checkState(threadStartTimeMillis > 0, "Thread time has not been recorded.");
    final long startupTimeMillis = threadStartTimeMillis - workerStartTimeMillis;
    Preconditions.checkState(startupTimeMillis >= 0,
        "Thread that works on fragment cannot run (%s) before query is initialized (%s).", workerStartTimeMillis,
        threadStartTimeMillis);
    final long threadStartNanos = operator.getFragment().getBeginNanoseconds();
    final long activeTimeNanos = System.nanoTime() - threadStartNanos;

    return TimeUnit.MILLISECONDS.toNanos(startupTimeMillis) + activeTimeNanos;
  }

  /**
   * Appends a single event appearing in an operator to a batch that is flushed either after a certain number of events
   * are in the batch or {@link #flushProfilingEventsBatch()} is called.
   * 
   * @param operator the operator where this record was logged
   * @param numTuples the number of tuples
   * @param startTime the start time of the event in ns
   * 
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordEvent(final Operator operator, final long numTuples, final long startTime)
      throws DbException {

    try {
      statementEvent.setLong(1, operator.getQueryId());
      statementEvent.setInt(2, operator.getFragmentId());
      statementEvent.setInt(3, operator.getOpId());
      statementEvent.setLong(4, startTime);
      statementEvent.setLong(5, getTime(operator));
      statementEvent.setLong(6, numTuples);

      statementEvent.addBatch();
      batchSizeEvents++;
    } catch (final SQLException e) {
      throw new DbException(e);
    }

    if (batchSizeEvents > MyriaConstants.PROFILING_LOGGER_BATCH_SIZE) {
      flushProfilingEventsBatch();
    }
  }

  /**
   * Flush the tuple batch buffer and transform the profiling data.
   * 
   * @throws DbException if insertion in the database fails
   */
  public synchronized void flush() throws DbException {
    flushSentBatch();
    flushProfilingEventsBatch();
    flushProfilingResourceBatch();
  }

  /**
   * Flush the tuple batch buffer that has logs about operator states.
   * 
   * @throws DbException if insertion in the database fails
   */
  private void flushProfilingEventsBatch() throws DbException {
    final long startTime = System.nanoTime();
    try {
      if (batchSizeEvents == 0) {
        return;
      }
      statementEvent.executeBatch();
      statementEvent.clearBatch();
      batchSizeEvents = 0;
    } catch (SQLException e) {
      if (e instanceof BatchUpdateException) {
        LOGGER.error("Error writing batch: ", e.getNextException());
      }
      throw new DbException(e);
    }
    LOGGER.info("Flushing the profiling events batch took {} milliseconds.", TimeUnit.NANOSECONDS.toMillis(System
        .nanoTime()
        - startTime));
  }

  /**
   * Flush the tuple batch buffer that has logs about resouce usage.
   * 
   */
  private void flushProfilingResourceBatch() {
    final long startTime = System.nanoTime();
    try {
      if (batchSizeResource == 0) {
        return;
      }
      statementResource.executeBatch();
      statementResource.clearBatch();
      batchSizeResource = 0;
    } catch (SQLException e) {
      if (e instanceof BatchUpdateException) {
        LOGGER.error("Error writing batch: ", e.getNextException());
      }
      throw new RuntimeException(e);
    }
    LOGGER.info("Flushing the profiling resource batch took {} milliseconds.", TimeUnit.NANOSECONDS.toMillis(System
        .nanoTime()
        - startTime));
  }

  /**
   * Flush the tuple batch buffer that has records about sent tuples.
   * 
   * @throws DbException if insertion in the database fails
   */
  private void flushSentBatch() throws DbException {
    final long startTime = System.nanoTime();
    try {
      if (batchSizeSent > 0) {
        statementSent.executeBatch();
        statementSent.clearBatch();
        batchSizeSent = 0;
      }
    } catch (SQLException e) {
      if (e instanceof BatchUpdateException) {
        LOGGER.error("Error writing batch: ", e.getNextException());
      }
      throw new DbException(e);
    }
    LOGGER.info("Flushing the sent batch took {} milliseconds.", TimeUnit.NANOSECONDS.toMillis(System.nanoTime()
        - startTime));
  }

  /**
   * Record that data was sent to a worker.
   * 
   * @param operator the operator where this record was logged
   * @param numTuples the number of tuples sent.
   * @param destWorkerId the worker if that we send the data to
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordSent(final Operator operator, final int numTuples, final int destWorkerId)
      throws DbException {
    try {
      statementSent.setLong(1, operator.getQueryId());
      statementSent.setInt(2, operator.getFragmentId());
      statementSent.setLong(3, getTime(operator));
      statementSent.setLong(4, numTuples);
      statementSent.setInt(5, destWorkerId);

      statementSent.addBatch();
      batchSizeSent++;
    } catch (final SQLException e) {
      throw new DbException(e);
    }

    if (batchSizeSent > MyriaConstants.PROFILING_LOGGER_BATCH_SIZE) {
      flushSentBatch();
    }
  }

  /**
   * Appends a single resource stats to a batch that is flushed either after a certain number of events are in the batch
   * or {@link #flushProfilingResourceBatch()} is called.
   * 
   * @param stats the resource stats.
   */
  public synchronized void recordResource(final ResourceStats stats) {

    try {
      statementResource.setLong(1, stats.getTimestamp());
      statementResource.setInt(2, stats.getOpId());
      statementResource.setString(3, stats.getMeasurement());
      statementResource.setLong(4, stats.getValue());
      statementResource.setLong(5, stats.getQueryId());
      statementResource.setLong(6, stats.getSubqueryId());
      statementResource.addBatch();
      batchSizeResource++;
    } catch (final SQLException e) {
      throw new RuntimeException(e);
    }

    if (batchSizeResource > MyriaConstants.PROFILING_LOGGER_BATCH_SIZE) {
      flushProfilingResourceBatch();
    }
  }

  /**
   * Returns {@code true} if the current JDBC connection is active.
   * 
   * @return {@code true} if the current JDBC connection is active.
   */
  public boolean isValid() {
    try {
      return connection.isValid(1);
    } catch (SQLException e) {
      LOGGER.warn("Error checking connection validity", e);
      return false;
    }
  }
}
