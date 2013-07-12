package edu.washington.escience.myriad.parallel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.util.ArrayUtils;

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

  /** The logger for this class. */
  static final Logger LOGGER = LoggerFactory.getLogger(ShuffleConsumer.class);

  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param workerIDs from which workers the data will come.
   * */
  public ShuffleConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    this(schema, operatorID, ArrayUtils.checkSet(org.apache.commons.lang3.ArrayUtils.toObject(workerIDs)));
  }

  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param workerIDs from which workers the data will come.
   * */
  public ShuffleConsumer(final Schema schema, final ExchangePairID operatorID, final ImmutableSet<Integer> workerIDs) {
    super(schema, operatorID, workerIDs);
    LOGGER.trace("created ShuffleConsumer for ExchangePairId=" + operatorID);
  }

}
