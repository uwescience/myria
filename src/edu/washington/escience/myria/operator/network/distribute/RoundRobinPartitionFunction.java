package edu.washington.escience.myria.operator.network.distribute;

import java.util.BitSet;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.storage.TupleBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A partition function that simply sends one tuple to each output in turn.
 */
public final class RoundRobinPartitionFunction extends PartitionFunction {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinPartitionFunction.class);
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The current partition to use. */
  private int curPartition = 0;

  @Override
  public TupleBatch[] partition(@Nonnull final TupleBatch tb) {
    BitSet[] partitions = new BitSet[numPartitions()];
    for (int i = 0; i < tb.numTuples(); i++) {
      LOGGER.info("Current partition: " + curPartition);
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
