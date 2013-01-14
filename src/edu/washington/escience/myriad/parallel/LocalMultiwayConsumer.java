package edu.washington.escience.myriad.parallel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.parallel.Exchange.ExchangePairID;

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
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  public LocalMultiwayConsumer(final LocalMultiwayProducer child, final ExchangePairID operatorID, final int[] workerIDs)
      throws DbException {
    super(child, operatorID, workerIDs);
  }

  public LocalMultiwayConsumer(final Schema schema, final ExchangePairID operatorID, final int[] workerIDs) {
    super(schema, operatorID, workerIDs);
  }
}
