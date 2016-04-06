/**
 *
 */
package edu.washington.escience.myria.operator.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;

/**
 * 
 */
public class CacheShuffleProducer extends GenericShuffleProducer {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectConsumer.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Constructor for the CacheShuffleProducer, just extends from the GenericShuffleProducer.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param workerIDs set of destination workers
   * @param pf the partition function
   */
  public CacheShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction pf) {
    super(child, operatorID, workerIDs, pf);
  }

}
