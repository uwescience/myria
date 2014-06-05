package edu.washington.escience.myria.parallel;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
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
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A logger for profiling data.
 */
public class ProfilingLogger {
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(LocalFragment.class.getName());

  /** The connection to the database database. */
  private final JdbcAccessMethod accessMethod;

  /** The jdbc connection. */
  private final Connection connection;

  /** A query statement for batching. */
  private PreparedStatement statementEvent;

  /** A query statement for batching. */
  private PreparedStatement statementSent;

  /**
   * A query statement for transforming the profiling relation from {@link MyriaConstants.PROFILING_SCHEMA_TMP} to
   * {@link MyriaConstants.PROFILING_SCHEMA}.
   */
  private PreparedStatement statementTransform;

  /** Number of rows in batch in {@link #statementEvent}. */
  private int batchSizeEvents = 0;

  /** Number of rows in batch in {@link #statementSent}. */
  private int batchSizeSent = 0;

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

    accessMethod.createUnloggedTableIfNotExists(MyriaConstants.PROFILING_RELATION_TMP,
        MyriaConstants.PROFILING_SCHEMA_TMP);
    accessMethod.createTableIfNotExists(MyriaConstants.PROFILING_RELATION, MyriaConstants.PROFILING_SCHEMA);
    accessMethod.createTableIfNotExists(MyriaConstants.SENT_RELATION, MyriaConstants.SENT_SCHEMA);

    createProfilingIndexes();
    createTmpProfilingIndexes();
    createSentIndex();

    connection = accessMethod.getConnection();
    try {
      statementEvent =
          connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.PROFILING_SCHEMA_TMP,
              MyriaConstants.PROFILING_RELATION_TMP));
    } catch (SQLException e) {
      throw new DbException(e);
    }

    try {
      statementTransform = connection.prepareStatement(getTransformProfilingDataStatement());
    } catch (SQLException e) {
      throw new DbException(e);
    }

    try {
      statementSent =
          connection.prepareStatement(accessMethod.insertStatementFromSchema(MyriaConstants.SENT_SCHEMA,
              MyriaConstants.SENT_RELATION));
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
  protected void createTmpProfilingIndexes() throws DbException {
    final Schema schema = MyriaConstants.PROFILING_SCHEMA_TMP;

    List<IndexRef> filterIndex = ImmutableList.of(IndexRef.of(schema, "queryId"));

    try {
      accessMethod.createIndexIfNotExists(MyriaConstants.PROFILING_RELATION_TMP, schema, filterIndex);
    } catch (DbException e) {
      LOGGER.error("Couldn't create index for profiling logs:", e);
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
  private long getTime(final Operator operator) {
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
   * Creates a statement for transforming the profiling relation from {@link MyriaConstants.PROFILING_SCHEMA_TMP} to
   * {@link MyriaConstants.PROFILING_SCHEMA}.
   * 
   * @return the insert into statement
   */
  private String getTransformProfilingDataStatement() {
    return Joiner.on(' ').join("INSERT INTO",
        MyriaConstants.PROFILING_RELATION.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
        "SELECT c.queryid, c.fragmentid, c.opid, c.nanotime as startTime, r.nanotime as endTime, r.numtuples", "FROM",
        MyriaConstants.PROFILING_RELATION_TMP.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL), "c,",
        MyriaConstants.PROFILING_RELATION_TMP.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL), "r", "WHERE",
        "c.queryid = r.queryid AND c.opid = r.opid AND c.fragmentid = r.fragmentid AND c.traceid = r.traceid",
        "AND r.eventtype = 'return' AND c.eventtype = 'call' AND c.queryid = ?", "ORDER  BY c.nanotime ASC;",
        "DELETE FROM", MyriaConstants.PROFILING_RELATION_TMP.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
        "WHERE", "queryid = ?");
  }

  /**
   * Appends a single event appearing in an operator to a batch that is flushed either after a certain number of events
   * are in the batch or {@link #flushProfilingEventsBatch()} is called.
   * 
   * @param operator the operator where this record was logged
   * @param numTuples the number of tuples
   * @param eventType the type of the event to be logged
   * @param traceId an id to trace corresponding events
   * 
   * @throws DbException if insertion in the database fails
   */
  public synchronized void recordEvent(final Operator operator, final long numTuples, final String eventType,
      final long traceId) throws DbException {

    try {
      statementEvent.setLong(1, operator.getQueryId());
      statementEvent.setInt(2, operator.getFragmentId());
      statementEvent.setInt(3, operator.getOpId());
      statementEvent.setLong(4, getTime(operator));
      statementEvent.setLong(5, numTuples);
      statementEvent.setString(6, eventType);
      statementEvent.setLong(7, traceId);

      statementEvent.addBatch();
      batchSizeEvents++;
    } catch (final SQLException e) {
      throw new DbException(e);
    }

    if (batchSizeEvents > TupleBatch.BATCH_SIZE) {
      flushProfilingEventsBatch();
    }
  }

  /**
   * Flush the tuple batch buffer and transform the profiling data.
   * 
   * @param queryId the query id
   * @throws DbException if insertion in the database fails
   */
  public synchronized void flush(final long queryId) throws DbException {
    flushSentBatch();
    flushProfilingEventsBatch();
    transformProfilingRelation(queryId);
  }

  /**
   * Executes the transformation.
   * 
   * @param queryId the query id
   * @throws DbException if any error occurs
   */
  private void transformProfilingRelation(final long queryId) throws DbException {
    try {
      statementTransform.setLong(1, queryId); // for insert statement
      statementTransform.setLong(2, queryId); // for delete statement
      connection.setAutoCommit(false);
      statementTransform.executeUpdate();
      connection.commit();
      connection.setAutoCommit(true);
    } catch (SQLException e) {
      if (e instanceof BatchUpdateException) {
        LOGGER.error("Error transforming profiling relation: ", e.getNextException());
      }
      throw new DbException(e);
    }
  }

  /**
   * Flush the tuple batch buffer that has logs about operator states.
   * 
   * @throws DbException if insertion in the database fails
   */
  private void flushProfilingEventsBatch() throws DbException {
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
  }

  /**
   * Flush the tuple batch buffer that has records about sent tuples.
   * 
   * @throws DbException if insertion in the database fails
   */
  private void flushSentBatch() throws DbException {
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

    if (batchSizeSent > TupleBatch.BATCH_SIZE) {
      flushSentBatch();
    }
  }
}
