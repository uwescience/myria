package edu.washington.escience.myria.operator.network;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.ExchangePairID;

/**
 * The consumer part of the Shuffle Exchange operator.
 *
 * A ShuffleProducer operator sends tuples to all the workers according to some PartitionFunction, while the
 * ShuffleConsumer (this class) encapsulates the methods to collect the tuples received at the worker from multiple
 * source workers' ShuffleProducer.
 *
 */
public final class LocalShuffleConsumer extends Consumer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param schema input/output data schema
   * @param operatorID source operatorID
   * */
  public LocalShuffleConsumer(final Schema schema, final ExchangePairID operatorID) {
    super(schema, operatorID);
  }
}
