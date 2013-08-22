package edu.washington.escience.myria.parallel;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.util.ArrayUtils;

/**
 * GenericShuffleProducer, which support json encoding of 1. Broadcast Shuffle 2. One to one Shuffle (Shuffle) 3. Hyper
 * Cube Join Shuffle (HyperJoinShuffle)
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class GenericShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the partition function.
   * */
  private final PartitionFunction<?, ?> partitionFunction;

  /**
   * partition of cells.
   */
  private final int[][] cellPartition;

  /**
   * one to one shuffle.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param workerIDs set of destination workers
   * @param pf the partition function
   */
  public GenericShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction<?, ?> pf) {
    this(child, operatorID, ArrayUtils.create2DIndex(workerIDs.length), workerIDs, pf);
  }

  /**
   * one to many shuffle.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param cellPartition buckets of destination workers the data goes.
   * @param workerIDs set of destination workers
   * @param pf the partition function
   * */
  public GenericShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[][] cellPartition,
      final int[] workerIDs, final PartitionFunction<?, ?> pf) {
    super(child, operatorID, workerIDs);
    partitionFunction = pf;
    this.cellPartition = cellPartition;
  }

  /**
   * @return return partition function.
   * */
  public final PartitionFunction<?, ?> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tup) throws DbException {
    TupleBatchBuffer[] buffers = getBuffers();
    tup.partition(partitionFunction, buffers);
    TupleBatch dm = null;
    for (int i = 0; i < cellPartition.length; i++) {
      final TupleBatchBuffer etb = buffers[i];
      while ((dm = etb.popAnyUsingTimeout()) != null) {
        for (int j : cellPartition[i]) {
          writeMessage(j, dm);
        }
      }
    }

  }

  @Override
  protected final void childEOS() throws DbException {
    TupleBatch dm = null;
    TupleBatchBuffer[] buffers = getBuffers();

    for (int i = 0; i < cellPartition.length; i++) {
      while ((dm = buffers[i].popAny()) != null) {
        for (int j : cellPartition[i]) {
          writeMessage(j, dm);
        }
      }
    }

    for (int p = 0; p < numChannels(); p++) {
      super.channelEnds(p);
    }

  }

  @Override
  protected final void childEOI() throws DbException {
    TupleBatch dm = null;
    TupleBatchBuffer[] buffers = getBuffers();
    TupleBatch eoiTB = TupleBatch.eoiTupleBatch(getSchema());

    for (int i = 0; i < cellPartition.length; i++) {
      while ((dm = buffers[i].popAny()) != null) {
        for (int j : cellPartition[i]) {
          writeMessage(j, dm);
        }
      }
    }

    for (int[] element : cellPartition) {
      for (int j : element) {
        writeMessage(j, eoiTB);
      }
    }
  }
}
