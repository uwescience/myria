package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * The producer part of the Collect Exchange operator.
 * 
 * The producer actively pushes the tuples generated by the child operator to the paired LocalMultiwayConsumer.
 * 
 */
public final class LocalMultiwayProducer extends GenericShuffleProducer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * */
  public LocalMultiwayProducer(final Operator child, final ExchangePairID[] operatorIDs) {
    super(child, operatorIDs, MyriaArrayUtils.create2DHorizontalIndex(operatorIDs.length),
        new int[] { IPCConnectionPool.SELF_IPC_ID }, new FixValuePartitionFunction(0), false);
  }

  @Override
  protected TupleBatch[] getTupleBatchPartitions(final TupleBatch tup) {
    return new TupleBatch[] { tup };
  }
}
