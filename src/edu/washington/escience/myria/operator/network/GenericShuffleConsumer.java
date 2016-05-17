package edu.washington.escience.myria.operator.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * Generic Shuffle Consumer, which supports the encoding of
 *
 * 1. BroadcastConsumer
 *
 * 2. ShuffleConsumer
 *
 * 3.HyperJoinShuffleConsumer
 *
 */
public class GenericShuffleConsumer extends Consumer {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for this class. */
  static final Logger LOGGER = LoggerFactory.getLogger(GenericShuffleConsumer.class);

  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param workerIDs from which workers the data will come.
   * */
  public GenericShuffleConsumer(
      final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    this(
        schema,
        operatorID,
        MyriaArrayUtils.checkSet(org.apache.commons.lang3.ArrayUtils.toObject(workerIDs)));
  }

  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param workerIDs from which workers the data will come.
   * */
  public GenericShuffleConsumer(
      final Schema schema, final ExchangePairID operatorID, final ImmutableSet<Integer> workerIDs) {
    super(schema, operatorID, workerIDs);
    LOGGER.trace("created GenericShuffleConsumer for ExchangePairId=" + operatorID);
  }
}
