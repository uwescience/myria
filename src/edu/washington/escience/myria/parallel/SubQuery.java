package edu.washington.escience.myria.parallel;

import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.operator.DbReader;
import edu.washington.escience.myria.operator.DbWriter;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;

/**
 * Represents a single {@link SubQuery} in a query.
 */
public final class SubQuery extends MetaTask {
  /** The id of this {@link SubQuery}. */
  private SubQueryId subQueryId;
  /** The master plan for this {@link SubQuery}. */
  private final SubQueryPlan masterPlan;
  /** The worker plans for this {@link SubQuery}. */
  private final Map<Integer, SubQueryPlan> workerPlans;
  /** The set of relations that this {@link SubQuery} reads. */
  private final Set<RelationKey> readRelations;
  /** The set of relations that this {@link SubQuery} writes. */
  private final Set<RelationKey> writeRelations;
  /** The execution statistics about this {@link SubQuery}. */
  private final ExecutionStatistics executionStats;

  /**
   * Construct a new {@link SubQuery} object for this {@link SubQuery}, with pending {@link SubQueryId}.
   * 
   * @param masterPlan the master's {@link SubQueryPlan}
   * @param workerPlans the {@link SubQueryPlan} for each worker
   */
  public SubQuery(final SubQueryPlan masterPlan, final Map<Integer, SubQueryPlan> workerPlans) {
    this(null, masterPlan, workerPlans);
  }

  /**
   * Construct a new {@link SubQuery} object for this {@link SubQuery}.
   * 
   * @param subQueryId the id of this {@link SubQuery}
   * @param masterPlan the master's {@link SubQueryPlan}
   * @param workerPlans the {@link SubQueryPlan} for each worker
   */
  public SubQuery(@Nullable final SubQueryId subQueryId, final SubQueryPlan masterPlan,
      final Map<Integer, SubQueryPlan> workerPlans) {
    this.subQueryId = subQueryId;
    this.masterPlan = Objects.requireNonNull(masterPlan, "masterPlan");
    this.workerPlans = Objects.requireNonNull(workerPlans, "workerPlans");
    executionStats = new ExecutionStatistics();

    ImmutableSet.Builder<RelationKey> read = ImmutableSet.builder();
    ImmutableSet.Builder<RelationKey> write = ImmutableSet.builder();
    computeReadWriteSets(read, write, masterPlan);
    for (SubQueryPlan plan : workerPlans.values()) {
      computeReadWriteSets(read, write, plan);
    }
    readRelations = read.build();
    writeRelations = write.build();
  }

  /**
   * Return the id of this {@link SubQuery}.
   * 
   * @return the id of this {@link SubQuery}
   */
  public SubQueryId getSubQueryId() {
    return subQueryId;
  }

  /**
   * Return the master's plan for this {@link SubQuery}.
   * 
   * @return the master's plan for this {@link SubQuery}
   */
  public SubQueryPlan getMasterPlan() {
    return masterPlan;
  }

  /**
   * Return the worker plans for this {@link SubQuery}.
   * 
   * @return the worker plans for this {@link SubQuery}
   */
  public Map<Integer, SubQueryPlan> getWorkerPlans() {
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
      final ImmutableSet.Builder<RelationKey> write, final SubQueryPlan plan) {
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
   * Returns the set of relations that are read when executing this {@link SubQuery}.
   * 
   * @return the set of relations that are read when executing this {@link SubQuery}
   */
  public Set<RelationKey> getReadRelations() {
    return readRelations;
  }

  /**
   * Returns the set of relations that are written when executing this {@link SubQuery}.
   * 
   * @return the set of relations that are written when executing this {@link SubQuery}
   */
  public Set<RelationKey> getWriteRelations() {
    return writeRelations;
  }

  /**
   * Returns the time this {@link SubQuery} started, in ISO8601 format, or <code>null</code> if the {@link SubQuery} has
   * not yet been started.
   * 
   * @return the time this {@link SubQuery} started, in ISO8601 format, or <code>null</code> if the {@link SubQuery} has
   *         not yet been started
   */
  public String getStartTime() {
    return executionStats.getStartTime();
  }

  /**
   * Set the time this {@link SubQuery} started to now in ISO8601 format.
   */
  public void markStart() {
    executionStats.markStart();
  }

  /**
   * Set the time this {@link SubQuery} ended to now in ISO8601 format.
   */
  public void markEnd() {
    executionStats.markEnd();
  }

  /**
   * Returns the time this {@link SubQuery} ended, in ISO8601 format, or <code>null</code> if the {@link SubQuery} has
   * not yet ended.
   * 
   * @return the time this {@link SubQuery} ended, in ISO8601 format, or <code>null</code> if the {@link SubQuery} has
   *         not yet ended
   */
  public String getEndTime() {
    return executionStats.getEndTime();
  }

  /**
   * Returns the time elapsed (in nanoseconds) since the {@link SubQuery} started, or <code>null</code> if the
   * {@link SubQuery} has not yet been started.
   * 
   * @return the time elapsed (in nanoseconds) since the {@link SubQuery} started, or <code>null</code> if the
   *         {@link SubQuery} has not yet been started
   */
  public Long getElapsedTime() {
    return executionStats.getQueryExecutionElapse();
  }

  /**
   * Set the id of this {@link SubQuery}.
   * 
   * @param subQueryId the id of this {@link SubQuery}
   * @throws IllegalStateException if the {@link SubQuery}'s id has already been set
   */
  public void setSubQueryId(final SubQueryId subQueryId) {
    Preconditions.checkState(this.subQueryId == null, "subquery id already set");
    this.subQueryId = subQueryId;
  }

  @Override
  public void instantiate(final LinkedList<MetaTask> metaQ, final LinkedList<SubQuery> subQueryQ, final Server server) {
    MetaTask task = metaQ.peekFirst();
    Verify.verify(task == this, "this SubQuery %s should be the first object on the queue, not %s!", this, task);
    metaQ.removeFirst();
    subQueryQ.addFirst(new SubQuery(masterPlan, workerPlans));
  }
}
