package edu.washington.escience.myria.operator.network;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.util.MyriaArrayUtils;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectConsumer.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param workerIDs from which workers the data will come.
   * */
  public CollectConsumer(
      final Schema schema, final ExchangePairID operatorID, final Set<Integer> workerIDs) {
    super(schema, operatorID, workerIDs);
    LOGGER.trace("created CollectConsumer for ExchangePairId=" + operatorID);
  }

  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param workerIDs from which workers the data will come.
   * */
  public CollectConsumer(
      final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    this(
        schema,
        operatorID,
        MyriaArrayUtils.checkSet(org.apache.commons.lang3.ArrayUtils.toObject(workerIDs)));
  }
}
