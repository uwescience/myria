package edu.washington.escience.myria.operator.network;

import java.util.List;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * GenericShuffleProducer, which support json encoding of 1. Broadcast Shuffle 2. One to one Shuffle (Shuffle) 3. Hyper
 * Cube Join Shuffle (HyperJoinShuffle)
 */
public class GenericShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** the distribute function. */
  protected final DistributeFunction distributeFunction;

  /**
   * Shuffle to multiple operator IDs on multiple workers. The most generic constructor.
   *
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * @param workerIDs set of destination workers
   * @param df the distribute function
   */
  public GenericShuffleProducer(
      final Operator child,
      final ExchangePairID[] operatorIDs,
      final int[] workerIDs,
      final DistributeFunction df) {
    super(child, operatorIDs, workerIDs);
    distributeFunction = df;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tup) throws DbException {
    final List<List<TupleBatch>> partitions = distributeFunction.distribute(tup);
    if (getProfilingMode().contains(ProfilingMode.QUERY)) {
      for (int channelIdx = 0; channelIdx < partitions.size(); channelIdx++) {
        int numTuples = 0;
        for (TupleBatch tb : partitions.get(channelIdx)) {
          if (tb != null) {
            numTuples += tb.numTuples();
          }
        }
        final int destWorkerId = getOutputIDs()[channelIdx].getRemoteID();
        getProfilingLogger().recordSent(this, numTuples, destWorkerId);
      }
    }
    writePartitionsIntoChannels(partitions);
  }

  @Override
  protected void childEOS() throws DbException {
    writePartitionsIntoChannels(null);
    for (int p = 0; p < numChannels(); p++) {
      super.channelEnds(p);
    }
  }

  @Override
  protected final void childEOI() throws DbException {
    writePartitionsIntoChannels(
        distributeFunction.distribute(TupleBatch.eoiTupleBatch(getSchema())));
  }

  /**
   * @return the distribute function
   */
  public final DistributeFunction getDistributeFunction() {
    return distributeFunction;
  }
}
