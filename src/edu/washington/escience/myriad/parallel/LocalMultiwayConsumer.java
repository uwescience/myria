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
 * The consumer passively collects Tuples from all the paired LocalMultiwayProducers
 * 
 */
public final class LocalMultiwayConsumer extends Consumer {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalMultiwayConsumer.class.getName());
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * If there's no child operator, a Schema is needed.
   * 
   * @param schema the input/output data schema
   * @param operatorID my operatorID
   */
  public LocalMultiwayConsumer(final Schema schema, final ExchangePairID operatorID) {
    super(schema, operatorID, new int[] { -1 });
    LOGGER.trace("created multiway consumer for ExchangePairID=" + operatorID);
  }

}
