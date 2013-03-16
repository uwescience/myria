package edu.washington.escience.myriad.parallel;

import java.util.Arrays;

import org.jboss.netty.channel.Channel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.util.IPCUtils;

public abstract class Producer extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The worker this operator is located at.
   * 
   */
  private transient IPCConnectionPool connectionPool;
  private transient Channel[] ioChannels;
  protected transient volatile long[] outputSeq;
  private transient TupleBatchBuffer[] buffers;

  private final ExchangePairID[] operatorIDs;
  private final int[] destinationWorkerIDs;

  @Override
  public void rewind(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffers = new TupleBatchBuffer[operatorIDs.length];
  }

  /**
   * no worker means to the owner worker.
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs) {
    this(child, oIDs, expandArray(new int[oIDs.length], -1)); // fill -1 at creation and change it at init.
  }

  /**
   * the same oID to different workers (shuffle or copy).
   * */
  public Producer(final Operator child, final ExchangePairID oID, final int[] destinationWorkerIDs) {
    this(child, (ExchangePairID[]) expandArray(new ExchangePairID[destinationWorkerIDs.length], oID),
        destinationWorkerIDs);
  }

  /**
   * same worker with different oIDs (multiway copy).
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs, final int destinationWorkerID) {
    this(child, oIDs, expandArray(new int[oIDs.length], Integer.valueOf(destinationWorkerID)));
  }

  /**
   * A single oID to a single worker (collect).
   * */
  public Producer(final Operator child, final ExchangePairID oID, final int destinationWorkerID) {
    this(child, new ExchangePairID[] { oID }, new int[] { destinationWorkerID });
  }

  /**
   * oID and worker pairs.
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs, final int[] destinationWorkerIDs) {
    super(child);
    Preconditions.checkArgument(oIDs.length == destinationWorkerIDs.length);
    operatorIDs = oIDs;
    this.destinationWorkerIDs = destinationWorkerIDs;
  }

  @Override
  public final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    connectionPool = (IPCConnectionPool) execEnvVars.get("ipcConnectionPool");
    ioChannels = new Channel[operatorIDs.length];
    outputSeq = new long[operatorIDs.length];
    buffers = new TupleBatchBuffer[operatorIDs.length];
    for (int i = 0; i < operatorIDs.length; i++) {
      ioChannels[i] = connectionPool.reserveLongTermConnection(destinationWorkerIDs[i]);
      ioChannels[i].write(IPCUtils.bosTM(operatorIDs[i]));
      outputSeq[i] = 0;
      buffers[i] = new TupleBatchBuffer(getSchema());
    }
  }

  @Override
  public final void cleanup() throws DbException {
    for (int i = 0; i < destinationWorkerIDs.length; i++) {
      connectionPool.releaseLongTermConnection(ioChannels[i]);
      buffers[i] = null;
    }
    buffers = null;
    ioChannels = null;
    outputSeq = null;
  }

  protected IPCConnectionPool getConnectionPool() {
    return connectionPool;
  }

  public final ExchangePairID[] operatorIDs() {
    return operatorIDs;
  }

  /**
   * @param myWorkerID for parsing self-references.
   * */
  public final int[] getDestinationWorkerIDs(final int myWorkerID) {
    int[] result = new int[destinationWorkerIDs.length];
    int idx = 0;
    for (int workerID : destinationWorkerIDs) {
      if (workerID >= 0) {
        result[idx++] = workerID;
      } else {
        result[idx++] = myWorkerID;
      }
    }

    return result;
  }

  private final static Object[] expandArray(Object[] arr, Object e) {
    Arrays.fill(arr, e);
    return arr;
  }

  private final static int[] expandArray(int[] arr, int e) {
    Arrays.fill(arr, e);
    return arr;
  }

  protected final Channel[] getChannels() {
    return ioChannels;
  }

  protected final TupleBatchBuffer[] getBuffers() {
    return buffers;
  }
}
