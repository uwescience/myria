package edu.washington.escience.myria.parallel;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.operator.DbReader;
import edu.washington.escience.myria.operator.DbWriter;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;

/**
 * Represents a single subquery in a query.
 */
public final class QueryTask {
  /** The id of this query. */
  private QueryTaskId taskId;
  /** The master plan for this task. */
  private final SingleQueryPlanWithArgs masterPlan;
  /** The worker plans for this task. */
  private final Map<Integer, SingleQueryPlanWithArgs> workerPlans;
  /** The set of relations that this query reads. */
  private final Set<RelationKey> readRelations;
  /** The set of relations that this query writes. */
  private final Set<RelationKey> writeRelations;
  /** The execution statistics about this query. */
  private final QueryExecutionStatistics executionStats;

  /**
   * Construct a new {@link QueryTask} object for this task, with pending task id.
   * 
   * @param masterPlan the master query plan
   * @param workerPlans the worker query plans
   */
  public QueryTask(final SingleQueryPlanWithArgs masterPlan, final Map<Integer, SingleQueryPlanWithArgs> workerPlans) {
    this(null, masterPlan, workerPlans);
  }

  /**
   * Construct a new {@link QueryTask} object for this task.
   * 
   * @param taskId the id of this task
   * @param masterPlan the master query plan
   * @param workerPlans the worker query plans
   */
  public QueryTask(@Nullable final QueryTaskId taskId, final SingleQueryPlanWithArgs masterPlan,
      final Map<Integer, SingleQueryPlanWithArgs> workerPlans) {
    this.taskId = taskId;
    this.masterPlan = Objects.requireNonNull(masterPlan, "masterPlan");
    this.workerPlans = Objects.requireNonNull(workerPlans, "workerPlans");
    executionStats = new QueryExecutionStatistics();

    ImmutableSet.Builder<RelationKey> read = ImmutableSet.builder();
    ImmutableSet.Builder<RelationKey> write = ImmutableSet.builder();
    computeReadWriteSets(read, write, masterPlan);
    for (SingleQueryPlanWithArgs plan : workerPlans.values()) {
      computeReadWriteSets(read, write, plan);
    }
    readRelations = read.build();
    writeRelations = write.build();
  }

  /**
   * Return the id of this task.
   * 
   * @return the id of this task
   */
  public QueryTaskId getTaskId() {
    return taskId;
  }

  /**
   * Return the master's plan for this query.
   * 
   * @return the master's plan for this query
   */
  public SingleQueryPlanWithArgs getMasterPlan() {
    return masterPlan;
  }

  /**
   * Return the worker plans for this query.
   * 
   * @return the worker plans for this query
   */
  public Map<Integer, SingleQueryPlanWithArgs> getWorkerPlans() {
    return workerPlans;
  }

  /**
   * A helper to walk various plan fragments and compute what relations they read and write. This is for understanding
   * query contention.
   * 
   * @param read a builder for the set of relations that are read
   * @param write a builder for the set of relations that are written
   * @param plan a single worker/master query plan
   */
  private static void computeReadWriteSets(final ImmutableSet.Builder<RelationKey> read,
      final ImmutableSet.Builder<RelationKey> write, final SingleQueryPlanWithArgs plan) {
    for (RootOperator op : plan.getRootOps()) {
      computeReadWriteSets(read, write, op);
    }
  }

  /**
   * A helper to walk various operators and compute what relations they read and write. This is for understanding query
   * contention.
   * 
   * @param read a builder for the set of relations that are read
   * @param write a builder for the set of relations that are written
   * @param op a single operator, which will be recursively traversed
   */
  private static void computeReadWriteSets(final ImmutableSet.Builder<RelationKey> read,
      final ImmutableSet.Builder<RelationKey> write, final Operator op) {
    if (op instanceof DbWriter) {
      write.addAll(((DbWriter) op).writeSet());
    } else if (op instanceof DbReader) {
      read.addAll(((DbReader) op).readSet());
    }

    for (Operator child : op.getChildren()) {
      computeReadWriteSets(read, write, child);
    }
  }

  /**
   * Returns the set of relations that are read when executing this query.
   * 
   * @return the set of relations that are read when executing this query
   */
  public Set<RelationKey> getReadRelations() {
    return readRelations;
  }

  /**
   * Returns the set of relations that are written when executing this query.
   * 
   * @return the set of relations that are written when executing this query
   */
  public Set<RelationKey> getWriteRelations() {
    return writeRelations;
  }

  /**
   * Returns the time this query started, in ISO8601 format, or null if the query has not yet been started.
   * 
   * @return the time this query started, in ISO8601 format, or null if the query has not yet been started
   */
  public String getStartTime() {
    return executionStats.getStartTime();
  }

  /**
   * Set the time this query started to now in ISO8601 format.
   */
  public void markStart() {
    executionStats.markQueryStart();
  }

  /**
   * Set the time this query ended to now in ISO8601 format.
   */
  public void markEnd() {
    executionStats.markQueryEnd();
  }

  /**
   * Returns the time this query ended, in ISO8601 format, or null if the query has not yet ended.
   * 
   * @return the time this query ended, in ISO8601 format, or null if the query has not yet ended
   */
  public String getEndTime() {
    return executionStats.getEndTime();
  }

  /**
   * Returns the time elapsed (in nanoseconds) since the query started.
   * 
   * @return the time elapsed (in nanoseconds) since the query started
   */
  public Long getElapsedTime() {
    return executionStats.getQueryExecutionElapse();
  }

  /**
   * Set the task id of this {@link QueryTask}.
   * 
   * @param taskId the task id
   * @throws IllegalStateException if the task id has already been set
   */
  public void setTaskId(final QueryTaskId taskId) {
    Preconditions.checkState(this.taskId == null, "task id already set");
    this.taskId = taskId;
  }
}
