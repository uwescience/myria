package edu.washington.escience.myria.operator.network.distribute;

import java.util.BitSet;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * The partition of a tuple is decided by the hash code of a group of fields of the tuple.
 */
public final class HashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The indices used for partitioning. */
  // @JsonProperty
  private final int[] indexes;

  /** The index of the chosen hashcode in <code>HashUtils</code>. */
  // @JsonProperty
  private final int seedIndex;

  /**
   * @param indexes the indices used for partitioning.
   */
  // @JsonCreator
  // public HashPartitionFunction(@JsonProperty("indexes") final int[] indexes) {
  public HashPartitionFunction(final int[] indexes) {
    this(indexes, 0);
  }

  /**
   * @param indexes the indices used for partitioning.
   * @param seedIndex the index of chosen hash seed.
   */
  public HashPartitionFunction(final int[] indexes, final int seedIndex) {
    Preconditions.checkArgument(
        indexes.length > 0, "HashPartitionFunction requires at least 1 field to hash");
    for (int i = 0; i < indexes.length; ++i) {
      Preconditions.checkArgument(
          indexes[i] >= 0,
          "HashPartitionFunction field index %s cannot take negative value %s",
          i,
          indexes[i]);
    }
    MyriaArrayUtils.checkSet(indexes);
    this.indexes = indexes;
    this.seedIndex = seedIndex % HashUtils.NUM_OF_HASHFUNCTIONS;
  }

  /**
   * @return the field indexes on which tuples will be hash partitioned.
   */
  public int[] getIndexes() {
    return indexes;
  }

  @Override
  public TupleBatch[] partition(@Nonnull final TupleBatch tb) {
    BitSet[] partitions = new BitSet[numPartitions()];
    for (int i = 0; i < partitions.length; ++i) {
      partitions[i] = new BitSet();
    }
    for (int i = 0; i < tb.numTuples(); i++) {
      int p = Math.floorMod(HashUtils.hashSubRow(tb, indexes, i, seedIndex), numPartitions());
      partitions[p].set(i);
    }
    TupleBatch[] tbs = new TupleBatch[numPartitions()];
    for (int i = 0; i < tbs.length; ++i) {
      tbs[i] = tb.filter(partitions[i]);
    }
    return tbs;
  }
}
