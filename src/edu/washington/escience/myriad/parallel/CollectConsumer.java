package edu.washington.escience.myriad.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.Schema;

/**
 * The consumer part of the Collect Exchange operator.
 * 
 * A Collect operator collects tuples from all the workers. There is a collect producer on each worker, and a collect
 * consumer on the server and a master worker if a master worker is needed.
 * 
 * The consumer passively collects Tuples from all the paired CollectProducers
 * 
 */
public final class CollectConsumer extends Consumer {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectConsumer.class.getName());

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  public CollectConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    super(schema, operatorID, workerIDs);
    LOGGER.trace("created CollectConsumer for ExchangePairId=" + operatorID);
  }

}
