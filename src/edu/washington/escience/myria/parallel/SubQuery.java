package edu.washington.escience.myria.parallel;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Represents a single {@link SubQuery} in a query.
 */
public final class SubQuery extends QueryPlan {
  /** The id of this {@link SubQuery}. */
  private SubQueryId subQueryId;
  /** The master plan for this {@link SubQuery}. */
  private final SubQueryPlan masterPlan;
  /** The worker plans for this {@link SubQuery}. */
  private final Map<Integer, SubQueryPlan> workerPlans;
  /** The set of relations that this {@link SubQuery} reads. */
  private final Set<RelationKey> readRelations;
  /** The set of relations that this {@link SubQuery} writes. */
  private final ImmutableMap<RelationKey, RelationWriteMetadata> writeRelations;
  /** The execution statistics about this {@link SubQuery}. */
  private final ExecutionStatistics executionStats;
  /** Whether this subquery can be profiled. */
  private final String planEncoding;

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
  public SubQuery(
      @Nullable final SubQueryId subQueryId,
      final SubQueryPlan masterPlan,
      final Map<Integer, SubQueryPlan> workerPlans) {
    this(subQueryId, masterPlan, workerPlans, null);
  }

  /**
   * Construct a new {@link SubQuery} object for this {@link SubQuery}.
   *
   * @param subQueryId the id of this {@link SubQuery}
   * @param masterPlan the master's {@link SubQueryPlan}
   * @param workerPlans the {@link SubQueryPlan} for each worker
   * @param planEncoding the encoding of the query plan, for subsequent recording.
   */
  public SubQuery(
      @Nullable final SubQueryId subQueryId,
      final SubQueryPlan masterPlan,
      final Map<Integer, SubQueryPlan> workerPlans,
      @Nullable final String planEncoding) {
    this.subQueryId = subQueryId;
    this.masterPlan = Objects.requireNonNull(masterPlan, "masterPlan");
    this.workerPlans = Objects.requireNonNull(workerPlans, "workerPlans");
    this.planEncoding = planEncoding;
    executionStats = new ExecutionStatistics();

    ImmutableSet.Builder<RelationKey> read =
        ImmutableSet.<RelationKey>builder().addAll(masterPlan.readSet());
    Map<RelationKey, RelationWriteMetadata> write = Maps.newHashMap();
    read.addAll(masterPlan.readSet());
    write.putAll(masterPlan.writeSet());
    for (SubQueryPlan plan : workerPlans.values()) {
      read.addAll(plan.readSet());
      MyriaUtils.putNewVerifyOld(plan.writeSet(), write);
    }
    readRelations = read.build();
    writeRelations = ImmutableMap.copyOf(write);
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
  public Map<RelationKey, RelationWriteMetadata> getWriteRelations() {
    return writeRelations;
  }

  /**
   * Returns the time this {@link SubQuery} started, in ISO8601 format, or <code>null</code> if the {@link SubQuery} has
   * not yet been started.
   *
   * @return the time this {@link SubQuery} started, in ISO8601 format, or <code>null</code> if the {@link SubQuery} has
   *         not yet been started
   */
  public DateTime getStartTime() {
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
  public DateTime getEndTime() {
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
    Preconditions.checkState(
        this.subQueryId == null,
        "subquery id already set to %s, not changing it to %s",
        this.subQueryId,
        subQueryId);
    this.subQueryId = subQueryId;
  }

  /**
   * Returns <code>true</code> if this subquery can be profiled.
   *
   * @return true if this subquery can be profiled.
   */
  public boolean isProfileable() {
    return planEncoding != null;
  }

  /**
   * Returns the encoded JSON query plan.
   *
   * @return the encoded JSON query plan.
   */
  public String getEncodedPlan() {
    return planEncoding;
  }

  @Override
  public void instantiate(
      final LinkedList<QueryPlan> planQ,
      final LinkedList<SubQuery> subQueryQ,
      final ConstructArgs args) {
    QueryPlan task = planQ.peekFirst();
    Verify.verify(
        task == this, "this %s should be the first object on the queue, not %s!", this, task);
    planQ.removeFirst();
    subQueryQ.addFirst(this);
  }

  /**
   * Returns a mapping showing what persistent relations this subquery will write, and all the associated
   * {@link RelationWriteMetadata} about these relations. This function is like {@link #getRelationWriteMetadata()}, but
   * returns only those relations that are persisted.
   *
   * @param server the {@link Server} on which this query will be executed.
   * @return a mapping showing what persistent relations are written and the corresponding {@link RelationWriteMetadata}
   *         .
   * @throws DbException if there is an error getting metadata about existing relations from the Server.
   */
  public Map<RelationKey, RelationWriteMetadata> getPersistentRelationWriteMetadata(
      final Server server) throws DbException {
    ImmutableMap.Builder<RelationKey, RelationWriteMetadata> ret = ImmutableMap.builder();
    for (Map.Entry<RelationKey, RelationWriteMetadata> entry :
        getRelationWriteMetadata(server).entrySet()) {
      RelationWriteMetadata meta = entry.getValue();
      if (!meta.isTemporary()) {
        ret.put(entry);
      }
    }
    return ret.build();
  }

  /**
   * Returns a mapping showing what relations this subquery will write, and all the associated
   * {@link RelationWriteMetadata} about these relations.
   *
   * @param server the {@link Server} on which this query will be executed.
   *
   * @return a mapping showing what relations are written and the corresponding {@link RelationWriteMetadata}.
   * @throws DbException if there is an error getting metadata about existing relations from the Server.
   */
  public Map<RelationKey, RelationWriteMetadata> getRelationWriteMetadata(final Server server)
      throws DbException {
    Map<RelationKey, RelationWriteMetadata> ret = new HashMap<>();

    /* Loop through each subquery plan, finding what relations it writes. */
    for (Map.Entry<Integer, SubQueryPlan> planEntry : workerPlans.entrySet()) {
      Integer workerId = planEntry.getKey();
      SubQueryPlan plan = planEntry.getValue();
      Map<RelationKey, RelationWriteMetadata> writes = plan.writeSet();

      for (Map.Entry<RelationKey, RelationWriteMetadata> writeEntry : writes.entrySet()) {
        RelationKey relation = writeEntry.getKey();
        RelationWriteMetadata meta = ret.get(relation);
        RelationWriteMetadata metadata = writeEntry.getValue();
        if (meta == null) {
          meta = metadata;
          ret.put(relation, meta);
        } else {
          /* We have an entry for this relation. Make sure that schema and overwrite match. */
          Preconditions.checkArgument(
              meta.isOverwrite() == metadata.isOverwrite(),
              "Cannot mix overwriting and appending to %s in the same subquery %s",
              relation,
              getSubQueryId());
          Preconditions.checkArgument(
              meta.getSchema().equals(metadata.getSchema()),
              "Cannot write to %s with two different Schemas %s and %s in the same subquery %s",
              relation,
              meta.getSchema(),
              metadata.getSchema(),
              getSubQueryId());
        }
        meta.addWorker(workerId);
      }
    }

    /*
     * Loop through all appending operators. Verify that the Schema does not change and ensure that the relation still
     * contains all workers.
     */
    for (Map.Entry<RelationKey, RelationWriteMetadata> metaEntry : ret.entrySet()) {
      RelationWriteMetadata metadata = metaEntry.getValue();
      if (metadata.isOverwrite()) {
        continue;
      }

      RelationKey relationKey = metaEntry.getKey();
      final long queryId = getSubQueryId().getQueryId();
      Schema schema = metadata.getSchema();
      Schema existingSchema;
      Set<Integer> existingWorkers;
      if (metadata.isTemporary()) {
        existingSchema = server.getTempSchema(queryId, relationKey.getRelationName());
        Preconditions.checkArgument(
            existingSchema != null,
            "Attempting to append to non-existent temporary relation %s",
            relationKey);
        existingWorkers = server.getWorkersForTempRelation(queryId, relationKey);
      } else {
        try {
          existingSchema = server.getSchema(relationKey);
          Preconditions.checkArgument(
              existingSchema != null,
              "Attempting to append to non-existent relation %s that is not in the Catalog",
              relationKey);
          existingWorkers = server.getWorkersForRelation(relationKey, null);
        } catch (CatalogException e) {
          throw new DbException(
              "Error verifying schema when appending to relation " + relationKey, e);
        }
      }
      Preconditions.checkArgument(
          schema.equals(existingSchema),
          "Cannot append to %s with changed Schema %s (old Schema: %s)",
          relationKey,
          schema,
          existingSchema);
      for (int w : existingWorkers) {
        metadata.addWorker(w);
      }
    }

    return ret;
  }

  /**
   * Resets this SubQuery so that it can be issued again. Needed for DoWhile.
   */
  @Override
  public void reset() {
    subQueryId = null;
    executionStats.reset();
  }
}
