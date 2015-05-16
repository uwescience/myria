/**
 *
 */
package edu.washington.escience.myria.operator.network;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.ExchangePairID;

/**
 * 
 */
public class CacheShuffleConsumer extends GenericShuffleConsumer {

  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param workerIDs from which workers the data will come.
   * */
  public CacheShuffleConsumer(final Schema schema, final ExchangePairID operatorID,
      final ImmutableSet<Integer> workerIDs) {
    super(schema, operatorID, workerIDs);
    // TODO Auto-generated constructor stub
  }

  // This class needs to open the CacheRoot operator and make this operator it's child

}
