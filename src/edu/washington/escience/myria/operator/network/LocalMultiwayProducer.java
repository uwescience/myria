package edu.washington.escience.myria.operator.network;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.network.distribute.BroadcastDistributeFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;

/** A producer that duplicates tuple batches to corresponding consumers. */
public final class LocalMultiwayProducer extends GenericShuffleProducer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   */
  public LocalMultiwayProducer(final Operator child, final ExchangePairID[] operatorIDs) {
    super(
        child,
        operatorIDs,
        new int[] {IPCConnectionPool.SELF_IPC_ID},
        new BroadcastDistributeFunction());
    this.distributeFunction.setDestinations(1, operatorIDs.length);
  }
}
