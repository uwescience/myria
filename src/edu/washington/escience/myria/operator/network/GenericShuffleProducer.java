package edu.washington.escience.myria.operator.network;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * GenericShuffleProducer, which support json encoding of 1. Broadcast Shuffle 2. One to one Shuffle (Shuffle) 3. Hyper
 * Cube Join Shuffle (HyperJoinShuffle)
 */
public class GenericShuffleProducer extends Producer {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the partition function.
   * */
  private final PartitionFunction partitionFunction;

  /**
   * Partition of cells.
   */
  private final int[][] partitionToChannel;

  /**
   * Shuffle to the same operator ID on multiple workers. (The old "ShuffleProducer")
   *
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param workerIDs set of destination workers
   * @param pf the partition function
   */
  public GenericShuffleProducer(
      final Operator child,
      final ExchangePairID operatorID,
      final int[] workerIDs,
      final PartitionFunction pf) {
    this(
        child,
        new ExchangePairID[] {operatorID},
        MyriaArrayUtils.create2DVerticalIndex(pf.numPartition()),
        workerIDs,
        pf,
        false);
    Preconditions.checkArgument(workerIDs.length == pf.numPartition());
  }

  /**
   * First partition data, then for each partition, send it to the set of workers in cellPartition[partition_id] using
   * the same operator ID. For BroadcastProducer and HyperShuffleProducer.
   *
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param cellPartition buckets of destination workers the data goes. The set of ids in cellPartition[i] means
   *          partition i should go to these workers. Since there's only one operator ID, cellPartition is also the
   *          mapping from partitions to ioChannels.
   * @param workerIDs set of destination workers
   * @param pf the partition function
   * */
  public GenericShuffleProducer(
      final Operator child,
      final ExchangePairID operatorID,
      final int[][] cellPartition,
      final int[] workerIDs,
      final PartitionFunction pf) {
    this(child, new ExchangePairID[] {operatorID}, cellPartition, workerIDs, pf, false);
    Preconditions.checkArgument(cellPartition.length == pf.numPartition());
  }

  /**
   * Shuffle to multiple operator IDs on multiple workers. The most generic constructor.
   *
   * @param child the child who provides data for this producer to distribute.
   * @param operatorIDs destination operators the data goes
   * @param partitionToChannel partitionToChannel[i] indicates ioChannels that partition i should be written into.
   * @param workerIDs set of destination workers
   * @param pf the partition function
   * @param isOneToOneMapping the same as the one in Producer
   * */
  public GenericShuffleProducer(
      final Operator child,
      final ExchangePairID[] operatorIDs,
      final int[][] partitionToChannel,
      final int[] workerIDs,
      final PartitionFunction pf,
      final boolean isOneToOneMapping) {
    super(child, operatorIDs, workerIDs, isOneToOneMapping);
    Preconditions.checkArgument(partitionToChannel.length == pf.numPartition());
    partitionFunction = pf;
    setNumOfPartition(pf.numPartition());
    this.partitionToChannel = partitionToChannel;
  }

  /**
   * @return return partition function.
   * */
  public final PartitionFunction getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tup) throws DbException {
    final TupleBatch[] partitions = getTupleBatchPartitions(tup);

    if (getProfilingMode().contains(ProfilingMode.QUERY)) {
      for (int partitionIdx = 0; partitionIdx < partitions.length; partitionIdx++) {
        if (partitions[partitionIdx] != null) {
          final int numTuples = partitions[partitionIdx].numTuples();
          for (int channelId : partitionToChannel[partitionIdx]) {
            final int destWorkerId = getOutputIDs()[channelId].getRemoteID();
            getProfilingLogger().recordSent(this, numTuples, destWorkerId);
          }
        }
      }
    }
    writePartitionsIntoChannels(true, partitionToChannel, partitions);
  }

  /**
   * call partition function to partition this tuple batch as an array of shallow copies of TupleBatch. subclasses can
   * override this method to have smarter partition approach.
   *
   * @param tb the tuple batch to be partitioned.
   * @return partitions.
   */
  protected TupleBatch[] getTupleBatchPartitions(final TupleBatch tb) {
    return tb.partition(partitionFunction);
  }

  @Override
  protected void childEOS() throws DbException {
    writePartitionsIntoChannels(false, partitionToChannel, null);
    for (int p = 0; p < numChannels(); p++) {
      super.channelEnds(p);
    }
  }

  @Override
  protected final void childEOI() throws DbException {
    TupleBatch[] partitions = new TupleBatch[getNumOfPartition()];
    for (int i = 0; i < getNumOfPartition(); i++) {
      partitions[i] = TupleBatch.eoiTupleBatch(getSchema());
    }
    writePartitionsIntoChannels(false, partitionToChannel, partitions);
  }
}
