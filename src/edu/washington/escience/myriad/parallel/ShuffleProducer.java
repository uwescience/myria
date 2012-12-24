package edu.washington.escience.myriad.parallel;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;

/**
 * The producer part of the Shuffle Exchange operator.
 * 
 * ShuffleProducer distributes tuples to the workers according to some partition function (provided as a
 * PartitionFunction object during the ShuffleProducer's instantiation).
 * 
 */
public class ShuffleProducer extends Producer {

  final class WorkingThread extends Thread {
    @Override
    public void run() {

      final int numWorker = workerIDs.length;
      final Channel[] shuffleChannels = new Channel[numWorker];
      int index = 0;
      for (final int workerID : workerIDs) {
        shuffleChannels[index] = getThisWorker().connectionPool.get(workerID, 3, null);
        index++;
      }
      Schema thisSchema = null;
      thisSchema = getSchema();

      try {
        TupleBatchBuffer[] buffers = new TupleBatchBuffer[numWorker];
        for (int i = 0; i < numWorker; i++) {
          buffers[i] = new TupleBatchBuffer(thisSchema);
        }
        TupleBatch tup = null;
        TransportMessage dm = null;
        while ((tup = child.next()) != null) {
          tup.partition(partitionFunction, buffers);
          for (int p = 0; p < numWorker; p++) {
            final TupleBatchBuffer etb = buffers[p];
            while ((dm = etb.popFilledAsTM(operatorID)) != null) {
              shuffleChannels[p].write(dm);
            }
          }
        }

        for (int i = 0; i < numWorker; i++) {
          while ((dm = buffers[i].popAnyAsTM(operatorID))!=null) {
            shuffleChannels[i].write(dm);
          }
        }

      } catch (final DbException e) {
        e.printStackTrace();
      }

      for (int i = 0; i < numWorker; i++) {
        shuffleChannels[i].write(IPCUtils.eosTM(operatorID));
      }
    }
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private transient WorkingThread runningThread;
  private final int[] workerIDs;

  private PartitionFunction<?, ?> partitionFunction;

  private Operator child;

  public ShuffleProducer(final Operator child, final ExchangePairID operatorID, final int[] workerIDs,
      final PartitionFunction<?, ?> pf) {
    super(operatorID);
    this.child = child;
    this.workerIDs = workerIDs;
    partitionFunction = pf;
  }

  @Override
  public final void cleanup() {
  }

  @Override
  protected final TupleBatch fetchNext() throws DbException {
    try {
      runningThread.join();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  public final PartitionFunction<?, ?> getPartitionFunction() {
    return partitionFunction;
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }

  @Override
  public final void init() throws DbException {
    runningThread = new WorkingThread();
    runningThread.start();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

  public final void setPartitionFunction(final PartitionFunction<?, ?> pf) {
    partitionFunction = pf;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return fetchNext();
  }

}
