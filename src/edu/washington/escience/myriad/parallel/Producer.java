package edu.washington.escience.myriad.parallel;

import java.util.Arrays;

import org.jboss.netty.channel.ChannelFuture;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.operator.RootOperator;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;
import edu.washington.escience.myriad.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myriad.parallel.ipc.IPCEvent;
import edu.washington.escience.myriad.parallel.ipc.IPCEventListener;
import edu.washington.escience.myriad.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myriad.parallel.ipc.StreamOutputChannel;

/**
 * A Producer is the counterpart of a consumer. It dispatch data using IPC channels to Consumers. Like network socket,
 * Each (workerID, operatorID) pair is a logical destination.
 * */
public abstract class Producer extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The worker this operator is located at.
   * 
   */
  private transient IPCConnectionPool connectionPool;
  /**
   * the netty channels doing the true IPC IO.
   * */
  private transient StreamOutputChannel<TupleBatch>[] ioChannels;

  /**
   * output buffers.
   * */
  private transient TupleBatchBuffer[] buffers;

  /**
   * output channel IDs.
   * */
  private final StreamIOChannelID[] outputIDs;

  /**
   * localized output stream channel IDs, with self references dereferenced.
   * */
  private transient StreamIOChannelID[] localizedOutputIDs;

  /**
   * if current query execution is in non-blocking mode.
   * */
  private transient boolean nonBlockingExecution;

  /**
   * no worker means to the owner worker.
   * 
   * @param child the child providing data.
   * @param oIDs operator IDs.
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs) {
    this(child, oIDs, arrayFillAndReturn(new int[oIDs.length], -1), true);
  }

  /**
   * the same oID to different workers (shuffle or copy).
   * 
   * @param oID the operator ID.
   * @param child the child providing data.
   * @param destinationWorkerIDs worker IDs.
   * 
   * */
  public Producer(final Operator child, final ExchangePairID oID, final int[] destinationWorkerIDs) {
    this(child, (ExchangePairID[]) arrayFillAndReturn(new ExchangePairID[destinationWorkerIDs.length], oID),
        destinationWorkerIDs, true);
  }

  /**
   * same worker with different oIDs (multiway copy).
   * 
   * @param oIDs the operator IDs.
   * @param child the child providing data.
   * @param destinationWorkerID the worker ID.
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs, final int destinationWorkerID) {
    this(child, oIDs, arrayFillAndReturn(new int[oIDs.length], Integer.valueOf(destinationWorkerID)), true);
  }

  /**
   * A single oID to a single worker (collect).
   * 
   * @param oID the operator ID.
   * @param child the child providing data.
   * @param destinationWorkerID the worker ID.
   * */
  public Producer(final Operator child, final ExchangePairID oID, final int destinationWorkerID) {
    this(child, new ExchangePairID[] { oID }, new int[] { destinationWorkerID }, true);
  }

  /**
   * Two modes:
   * <p>
   * 
   * <pre>
   * if (isOne2OneMapping)
   *    Each ( oIDs[i], destinationWorkerIDs[i] ) pair is a producer channel.
   *    It's required that oIDs.length==destinationWorkerIDs.length
   *    The number of producer channels is oID.length==destinationWorkerIDs.length
   * else
   *    Each combination of oID and workerID is a producer channel.
   *    The number of producer channels is oID.length*destinationWorkerIDs.length
   * </pre>
   * 
   * 
   * @param oIDs the operator IDs.
   * @param child the child providing data.
   * @param destinationWorkerIDs the worker IDs.
   * @param isOne2OneMapping choosing the mode.
   * 
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs, final int[] destinationWorkerIDs,
      final boolean isOne2OneMapping) {
    super(child);
    if (isOne2OneMapping) {
      // oID and worker pairs. each ( oIDs[i], destinationWorkerIDs[i] ) pair is a logical channel.
      Preconditions.checkArgument(oIDs.length == destinationWorkerIDs.length);
      outputIDs = new StreamIOChannelID[oIDs.length];
      for (int i = 0; i < oIDs.length; i++) {
        outputIDs[i] = new StreamIOChannelID(oIDs[i].getLong(), destinationWorkerIDs[i]);
      }
    } else {
      outputIDs = new StreamIOChannelID[oIDs.length * destinationWorkerIDs.length];
      int idx = 0;
      for (int wID : destinationWorkerIDs) {
        for (ExchangePairID oID : oIDs) {
          outputIDs[idx] = new StreamIOChannelID(oID.getLong(), wID);
          idx++;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    connectionPool = (IPCConnectionPool) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_IPC_CONNECTION_POOL);
    drivingTask = (QuerySubTreeTask) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DRIVING_TASK);
    ioChannels = new StreamOutputChannel[outputIDs.length];
    buffers = new TupleBatchBuffer[outputIDs.length];
    localizedOutputIDs = new StreamIOChannelID[outputIDs.length];
    for (int i = 0; i < outputIDs.length; i++) {
      if (outputIDs[i].getRemoteID() < 0) {
        localizedOutputIDs[i] = new StreamIOChannelID(outputIDs[i].getStreamID(), connectionPool.getMyIPCID());
      } else {
        localizedOutputIDs[i] = outputIDs[i];
      }
    }
    for (int i = 0; i < localizedOutputIDs.length; i++) {
      final int j = i;
      ioChannels[i] =
          connectionPool.reserveLongTermConnection(localizedOutputIDs[i].getRemoteID(), localizedOutputIDs[i]
              .getStreamID());
      ioChannels[i].addListener(StreamOutputChannel.OUTPUT_DISABLED, new IPCEventListener() {

        @Override
        public void triggered(final IPCEvent event) {
          drivingTask.notifyOutputDisabled(localizedOutputIDs[j]);
        }
      });
      ioChannels[i].addListener(StreamOutputChannel.OUTPUT_RECOVERED, new IPCEventListener() {

        @Override
        public void triggered(final IPCEvent event) {
          drivingTask.notifyOutputEnabled(localizedOutputIDs[j]);
        }
      });
      buffers[i] = new TupleBatchBuffer(getSchema());
    }

    QueryExecutionMode executionMode = (QueryExecutionMode) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE);
    nonBlockingExecution = (executionMode == QueryExecutionMode.NON_BLOCKING);
  }

  /**
   * @param chIdx the channel to write
   * @param msg the message.
   * @throws InterruptedException if interrupted
   * @return write future
   * */
  protected final ChannelFuture writeMessage(final int chIdx, final TupleBatch msg) throws InterruptedException {
    StreamOutputChannel<TupleBatch> ch = ioChannels[chIdx];
    if (nonBlockingExecution) {
      return ch.write(msg);
    } else {
      int sleepTime = 1;
      int maxSleepTime = MyriaConstants.SHORT_WAITING_INTERVAL_MS;
      while (true) {
        if (ch.isWritable()) {
          return ch.write(msg);
        } else {
          int toSleep = sleepTime - 1;
          if (maxSleepTime < sleepTime) {
            toSleep = maxSleepTime;
          }
          Thread.sleep(toSleep);
          sleepTime *= 2;
        }
      }
    }
  }

  /**
   * @param chIdx the channel to write
   * @return channel release future.
   * */
  protected final ChannelFuture channelEnds(final int chIdx) {
    return ioChannels[chIdx].release();
  }

  @Override
  public final void cleanup() throws DbException {
    for (int i = 0; i < localizedOutputIDs.length; i++) {
      ioChannels[i].release();
      buffers[i] = null;
    }
    buffers = null;
    ioChannels = null;
  }

  /**
   * @return my connection pool.
   * */
  protected final IPCConnectionPool getConnectionPool() {
    return connectionPool;
  }

  /**
   * The driving task of this operator.
   * */
  private transient QuerySubTreeTask drivingTask;

  /**
   * @param myWorkerID for parsing self-references.
   * @return destination worker IDs.
   * */
  public final StreamIOChannelID[] getOutputChannelIDs(final int myWorkerID) {
    StreamIOChannelID[] result = new StreamIOChannelID[outputIDs.length];
    int idx = 0;
    for (StreamIOChannelID ecID : outputIDs) {
      if (ecID.getRemoteID() < 0) {
        result[idx++] = ecID;
      } else {
        result[idx++] = new StreamIOChannelID(ecID.getStreamID(), myWorkerID);
      }
    }

    return result;
  }

  /**
   * Fill object array.
   * 
   * @return the filled array.
   * @param arr the array to fill
   * @param e the element to fill.
   * */
  private static Object[] arrayFillAndReturn(final Object[] arr, final Object e) {
    Arrays.fill(arr, e);
    return arr;
  }

  /**
   * Fill int array.
   * 
   * @return the filled array.
   * @param arr the array to fill
   * @param e the element to fill.
   * */
  private static int[] arrayFillAndReturn(final int[] arr, final int e) {
    Arrays.fill(arr, e);
    return arr;
  }

  /**
   * @return number of output channels.
   * */
  public final int numChannels() {
    return ioChannels.length;
  }

  /**
   * @return the buffers.
   * */
  protected final TupleBatchBuffer[] getBuffers() {
    return buffers;
  }

}
