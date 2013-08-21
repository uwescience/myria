package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.operator.Operator;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided as a
 * PartitionFunction object during the ShuffleProducer's instantiation).
 * 
 * @deprecated
 */
@Deprecated
public class ShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the partition function.
   * */
  private final PartitionFunction<?, ?> partitionFunction;

  /**
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param workerIDs destination workers the data goes.
   * @param pf the partition function
   * */
  public ShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction<?, ?> pf) {
    super(child, operatorID, workerIDs);
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
    TupleBatchBuffer[] buffers = getBuffers();
    tup.partition(partitionFunction, buffers);
    popTBsFromBuffersAndWrite(true);
  }

  @Override
  protected final void childEOS() throws DbException {
    popTBsFromBuffersAndWrite(false);
    for (int p = 0; p < numChannels(); p++) {
      super.channelEnds(p);
    }
  }

  @Override
  protected final void childEOI() throws DbException {
    TupleBatchBuffer[] buffers = getBuffers();
    for (int i = 0; i < numChannels(); i++) {
      buffers[i].appendTB(TupleBatch.eoiTupleBatch(getSchema()));
    }
    popTBsFromBuffersAndWrite(false);
  }
}
