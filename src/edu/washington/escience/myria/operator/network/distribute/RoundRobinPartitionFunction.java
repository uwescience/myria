package edu.washington.escience.myria.operator.network.distribute;

import java.util.BitSet;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A partition function that simply sends one tuple to each output in turn.
 */
public final class RoundRobinPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The current partition to use. */
  private int curPartition = 0;

  @Override
  public TupleBatch[] partition(@Nonnull final TupleBatch tb) {
    BitSet[] partitions = new BitSet[numPartitions()];
    for (int i = 0; i < partitions.length; ++i) {
      partitions[i] = new BitSet();
    }
    for (int i = 0; i < tb.numTuples(); i++) {
      partitions[curPartition].set(i);
      curPartition = (curPartition + 1) % numPartitions();
    }
    TupleBatch[] tbs = new TupleBatch[numPartitions()];
    for (int i = 0; i < tbs.length; ++i) {
      tbs[i] = tb.filter(partitions[i]);
    }
    return tbs;
  }
}
