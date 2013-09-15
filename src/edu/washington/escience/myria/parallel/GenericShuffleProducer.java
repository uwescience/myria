package edu.washington.escience.myria.parallel;

<<<<<<< HEAD
import org.slf4j.LoggerFactory;
=======
import com.google.common.base.Preconditions;
>>>>>>> d76d2ec... Adding constructors and updating their javadocs in GenericShuffleProducer.

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.util.MyriaArrayUtils;

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
  private final int[][] partitionToChannel;

  /**
<<<<<<< HEAD
   * The time spends on sending tuples via network.
   */
  private long shuffleNetworkTime = 0;

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(GenericShuffleProducer.class);

  /**
   * one to one shuffle.
=======
   * Shuffle to the same operator ID on multiple workers. (The old "ShuffleProducer")
>>>>>>> d76d2ec... Adding constructors and updating their javadocs in GenericShuffleProducer.
   * 
   * @param child the child who provides data for this producer to distribute.
   * @param operatorID destination operators the data goes
   * @param workerIDs set of destination workers
   * @param pf the partition function
   */
  public GenericShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction<?, ?> pf) {
    this(child, new ExchangePairID[] { operatorID }, MyriaArrayUtils.create2DVerticalIndex(pf.numPartition()),
        workerIDs, pf, false);
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
  public GenericShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[][] cellPartition,
      final int[] workerIDs, final PartitionFunction<?, ?> pf) {
    this(child, new ExchangePairID[] { operatorID }, cellPartition, workerIDs, pf, false);
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
  public GenericShuffleProducer(final Operator child, final ExchangePairID[] operatorIDs,
      final int[][] partitionToChannel, final int[] workerIDs, final PartitionFunction<?, ?> pf,
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
  public final PartitionFunction<?, ?> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  protected final void consumeTuples(final TupleBatch tup) throws DbException {
    TupleBatch[] partitions = tup.partition(partitionFunction);
    long startTime = System.currentTimeMillis();
    writePartitionsIntoChannels(true, partitionToChannel, partitions);
    shuffleNetworkTime += System.currentTimeMillis() - startTime;
  }

  @Override
  protected final void childEOS() throws DbException {
    writePartitionsIntoChannels(false, partitionToChannel, null);
    for (int p = 0; p < numChannels(); p++) {
      super.channelEnds(p);
    }

    if (isProfilingMode()) {
      LOGGER.info("[{}#{}]{}[{}]: shuffle network time {} ms", MyriaConstants.EXEC_ENV_VAR_QUERY_ID, getQueryId(),
          getOpName(), shuffleNetworkTime);
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
