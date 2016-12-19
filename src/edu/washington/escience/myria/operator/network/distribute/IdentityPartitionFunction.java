package edu.washington.escience.myria.operator.network.distribute;

import java.util.BitSet;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Implementation that uses multiple fields as the key to hash The partition of
 * a tuple is decided by the hash code of a group of fields of the tuple.
 */
public final class IdentityPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** the column index for partitioning. */
  @JsonProperty private final int index;

  /**
   * @param index the column index for partitioning.
   */
  public IdentityPartitionFunction(final int index) {
    this.index = index;
  }

  @Override
  public TupleBatch[] partition(@Nonnull final TupleBatch tb) {
    BitSet[] partitions = new BitSet[numPartition()];
    for (int i = 0; i < tb.numTuples(); i++) {
      partitions[tb.getInt(index, i) - 1].set(i);
    }
    TupleBatch[] tbs = new TupleBatch[numPartition()];
    for (int i = 0; i < tbs.length; ++i) {
      tbs[i] = tb.filter(partitions[i]);
    }
    return tbs;
  }
}
