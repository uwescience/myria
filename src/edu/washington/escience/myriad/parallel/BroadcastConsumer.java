package edu.washington.escience.myriad.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.Schema;

/**
 * 
 * The consumer part of broadcast operator
 * 
 * BroadcastProducer send multiple copies of tuples to a set of workers. Each worker will receive the same tuples
 * 
 * @author Shumo Chu (chushumo@cs.washington.edu)
 * 
 */

public final class BroadcastConsumer extends Consumer {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(BroadcastConsumer.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param schema input/output data schema
   * @param operatorID operator id of current exchange operator
   * @param workerIDs source worker IDs of broadcasted tuples
   */
  public BroadcastConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    super(schema, operatorID, workerIDs);
    LOGGER.trace("created BroadcastConsumer for ExchangePairId=" + operatorID);
  }

}
