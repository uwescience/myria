/**
 *
 */
package edu.washington.escience.myria.operator.network;

import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;

/**
 * 
 */
public class CacheShuffleProducer extends GenericShuffleProducer {

  /**
   * Shuffle to the same operator ID on multiple workers. (The old "ShuffleProducer")
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param workerIDs set of destination workers
   * @param pf the partition function
   */
  public CacheShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction pf) {
    super(child, operatorID, workerIDs, pf);
    // TODO Auto-generated constructor stub
  }

}
