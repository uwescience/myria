package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.Operator;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided as a
 * PartitionFunction object during the ShuffleProducer's instantiation).
 * 
 */
public class LocalShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the partition function.
   * */
  private final PartitionFunction<?, ?> partitionFunction;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * @param pf the partition function
   * */
  public LocalShuffleProducer(final Operator child, final ExchangePairID[] operatorIDs, final PartitionFunction<?, ?> pf) {
    super(child, operatorIDs);
    partitionFunction = pf;
  }

  /**
   * @return the partition function I'm using.
   * */
  public final PartitionFunction<?, ?> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tup) throws DbException {
    TupleBatch[] tbs = tup.partition(partitionFunction);
    for (int p = 0; p < tbs.length; p++) {
      if (tbs[p] != null) {
        writeMessage(p, tbs[p]);
      }
    }
  }

  @Override
  protected final void childEOS() throws DbException {
    for (int i = 0; i < numChannels(); i++) {
      super.channelEnds(i);
    }
  }

  @Override
  protected final void childEOI() throws DbException {
    TupleBatch eoiTB = TupleBatch.eoiTupleBatch(getSchema());
    for (int i = 0; i < numChannels(); i++) {
      writeMessage(i, eoiTB);
    }
  }
}
