package edu.washington.escience.myria.operator.network;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * The producer part of the Shuffle Exchange operator.
 *
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided as a
 * PartitionFunction object during the ShuffleProducer's instantiation).
 *
 */
public class LocalShuffleProducer extends GenericShuffleProducer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * @param pf the partition function
   * */
  public LocalShuffleProducer(
      final Operator child, final ExchangePairID[] operatorIDs, final PartitionFunction pf) {
    super(
        child,
        operatorIDs,
        MyriaArrayUtils.create2DVerticalIndex(pf.numPartition()),
        new int[] {IPCConnectionPool.SELF_IPC_ID},
        pf,
        false);
    Preconditions.checkArgument(operatorIDs.length == pf.numPartition());
  }
}
