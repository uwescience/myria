package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

/**
 * The consumer part of the Shuffle Exchange operator.
 * 
 * A ShuffleProducer operator sends tuples to all the workers according to some PartitionFunction, while the
 * ShuffleConsumer (this class) encapsulates the methods to collect the tuples received at the worker from multiple
 * source workers' ShuffleProducer.
 * 
 */
public final class ShuffleConsumer extends Consumer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  public ShuffleConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    super(schema, operatorID, workerIDs);
  }
}
