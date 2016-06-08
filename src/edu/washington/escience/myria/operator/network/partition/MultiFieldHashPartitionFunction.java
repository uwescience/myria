package edu.washington.escience.myria.operator.network.partition;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Implementation that uses multiple fields as the key to hash
 *
 * The partition of a tuple is decided by the hash code of a group of fields of the tuple.
 */
public final class MultiFieldHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  @JsonProperty private final int[] indexes;

  /**
   * @param numPartition number of partitions
   * @param indexes the indices used for partitioning.
   */
  @JsonCreator
  public MultiFieldHashPartitionFunction(
      @Nullable @JsonProperty("numPartitions") final Integer numPartition,
      @JsonProperty(value = "indexes", required = true) final Integer[] indexes) {
    super(numPartition);
    Objects.requireNonNull(indexes, "indexes");
    Preconditions.checkArgument(
        indexes.length > 1, "MultiFieldHash requires at least 2 fields to hash");
    this.indexes = new int[indexes.length];
    for (int i = 0; i < indexes.length; ++i) {
      int index =
          Preconditions.checkNotNull(indexes[i], "MultiFieldHash field index %s cannot be null", i);
      Preconditions.checkArgument(
          index >= 0, "MultiFieldHash field index %s cannot take negative value %s", i, index);
      this.indexes[i] = index;
    }
  }

  /**
   * @param numPartition number of partitions
   * @param indexes the indices used for partitioning.
   */
  public MultiFieldHashPartitionFunction(final Integer numPartition, final int[] indexes) {
    super(numPartition);
    Preconditions.checkArgument(
        indexes.length > 1, "MultiFieldHash requires at least 2 fields to hash");
    this.indexes = indexes;
    for (int i = 0; i < indexes.length; ++i) {
      int index = indexes[i];
      Preconditions.checkArgument(
          index >= 0, "MultiFieldHash field index %s cannot take negative value %s", i, index);
    }
  }

  /**
   * @return the field indexes on which tuples will be hash partitioned.
   */
  public int[] getIndexes() {
    return indexes;
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = HashUtils.hashSubRow(tb, indexes, i) % numPartition();
      if (p < 0) {
        p = p + numPartition();
      }
      result[i] = p;
    }
    return result;
  }
}
