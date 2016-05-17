package edu.washington.escience.myria.parallel;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;

/**
 * Holds metadata about a relation that is created by a subquery.
 */
public class RelationWriteMetadata implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The relation. */
  private final RelationKey relationKey;
  /** The workers that will store the relation. */
  private final Set<Integer> workers;
  /** The schema of the relation. */
  private final Schema schema;
  /** Whether an existing copy of relation will be overwritten. */
  private final boolean overwrite;
  /** Whether the relation being written is a temporary or a permanent relation. */
  private final boolean temporary;
  /** The PartitionFunction used to partition the relation across workers. */
  private final PartitionFunction partitionFunction;

  /**
   * Constructs a new relation metadata object.
   *
   * @param relationKey the relation to be written
   * @param schema the schema of the write.
   * @param overwrite if {@code true}, then the relation will be overwritten / created. if false, the relation will be
   *          created or appended. If appending, the schema must match the catalog schema.
   * @param temporary if {@code true}, then the relation will be not be added to the Catalog, and its tuple count will
   *          not be maintained.
   */
  public RelationWriteMetadata(
      @Nonnull final RelationKey relationKey,
      @Nonnull final Schema schema,
      final boolean overwrite,
      final boolean temporary) {
    this(relationKey, schema, overwrite, temporary, null);
  }

  /**
   * Constructs a new relation metadata object.
   *
   * @param relationKey the relation to be written
   * @param schema the schema of the write.
   * @param overwrite if {@code true}, then the relation will be overwritten / created. if false, the relation will be
   *          created or appended. If appending, the schema must match the catalog schema.
   * @param temporary if {@code true}, then the relation will be not be added to the Catalog, and its tuple count will
   *          not be maintained.
   * @param partitionFunction the PartitionFunction used to partition the relation across workers.
   */
  public RelationWriteMetadata(
      @Nonnull final RelationKey relationKey,
      @Nonnull final Schema schema,
      final boolean overwrite,
      final boolean temporary,
      @Nullable final PartitionFunction partitionFunction) {
    this.relationKey = Objects.requireNonNull(relationKey, "relationKey");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.overwrite = overwrite;
    this.temporary = temporary;
    this.partitionFunction = partitionFunction;
    workers = new HashSet<>();
  }

  /**
   * Add the specified worker to set of workers writing this relation.
   *
   * @param workerId the id of the worker
   */
  public void addWorker(final Integer workerId) {
    workers.add(workerId);
  }

  /**
   * Indicates whether the relation will be overwritten if it already exists. If {@code false} and the relation does
   * already exist, tuples will be appended to the relation and the schema of these tuples must match the schema already
   * in the catalog.
   *
   * @return {@code true} if the relation will be overwritten if it already exists, or {@code false} otherwise.
   */
  public boolean isOverwrite() {
    return overwrite;
  }

  /**
   * Indicates whether the relation is a temporary relation ({@code true}) or will be persisted ({@code false}). If (
   * {@code false}), the relation will not be persisted and/or added to the catalog, reducing query overhead.
   *
   * @return if {@code false}, the relation will be persisted and entered into the catalog.
   */
  public boolean isTemporary() {
    return temporary;
  }

  /**
   * Get the key of the relation to be written.
   *
   * @return the key of the relation to be written.
   */
  public RelationKey getRelationKey() {
    return relationKey;
  }

  /**
   * Get the schema of the tuples to be written to this relation.
   *
   * @return the schema of the tuples to be written to this relation
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Get the set of workers on which this relation will be written.
   *
   * @return the set of workers on which this relation will be written.
   */
  public Set<Integer> getWorkers() {
    return ImmutableSet.copyOf(workers);
  }

  /**
   * Get the PartitionFunction used to partition this relation.
   *
   * @return the PartitionFunction used to partition this relation
   */
  public PartitionFunction getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  public boolean equals(final Object other) {
    if ((other == null) || !(other instanceof RelationWriteMetadata)) {
      return false;
    }
    RelationWriteMetadata o = (RelationWriteMetadata) other;
    return Objects.equals(schema, o.schema)
        && Objects.equals(relationKey, o.relationKey)
        && Objects.equals(overwrite, o.overwrite);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relationKey, schema, overwrite);
  }
}
