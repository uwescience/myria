package edu.washington.escience.myria.parallel;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.channel.ChannelFuture;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMODE;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.util.ArrayUtils;

/**
 * A Producer is the counterpart of a consumer. It dispatch data using IPC channels to Consumers. Like network socket,
 * Each (workerID, operatorID) pair is a logical destination.
 * */
public abstract class Producer extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(Producer.class.getName());
  /**
   * The worker this operator is located at.
   */
  private transient TaskResourceManager taskResourceManager;

  /**
   * the netty channels doing the true IPC IO.
   * */
  private transient StreamOutputChannel<TupleBatch>[] ioChannels;
  /**
   * if the corresponding ioChannel is available to write again.
   * */
  private transient boolean[] ioChannelsAvail;

  /**
   * output buffers.
   * */
  private transient TupleBatchBuffer[] buffers;

  /**
   * output buffers.
   * */
  private transient List<List<TupleBatch>> backupBuffers;

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
    this(child, oIDs, ArrayUtils.arrayFillAndReturn(new int[oIDs.length], IPCConnectionPool.SELF_IPC_ID), true);
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
    this(child, (ExchangePairID[]) ArrayUtils.arrayFillAndReturn(new ExchangePairID[destinationWorkerIDs.length], oID),
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
    this(child, oIDs, ArrayUtils.arrayFillAndReturn(new int[oIDs.length], Integer.valueOf(destinationWorkerID)), true);
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
    taskResourceManager = (TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER);
    ioChannels = new StreamOutputChannel[outputIDs.length];
    ioChannelsAvail = new boolean[outputIDs.length];
    buffers = new TupleBatchBuffer[outputIDs.length];
    backupBuffers = new ArrayList<List<TupleBatch>>();
    localizedOutputIDs = new StreamIOChannelID[outputIDs.length];
    for (int i = 0; i < outputIDs.length; i++) {
      if (outputIDs[i].getRemoteID() == IPCConnectionPool.SELF_IPC_ID) {
        localizedOutputIDs[i] = new StreamIOChannelID(outputIDs[i].getStreamID(), taskResourceManager.getMyWorkerID());
      } else {
        localizedOutputIDs[i] = outputIDs[i];
      }
    }
    for (int i = 0; i < localizedOutputIDs.length; i++) {
      createANewChannel(i);
    }
    nonBlockingExecution = (taskResourceManager.getExecutionMode() == QueryExecutionMode.NON_BLOCKING);
  }

  /**
   * Does all the jobs needed to create a new channel with index i.
   * 
   * @param i the index of the channel
   * */
  public void createANewChannel(final int i) {
    ioChannels[i] =
        taskResourceManager.startAStream(localizedOutputIDs[i].getRemoteID(), localizedOutputIDs[i].getStreamID());
    ioChannels[i].addListener(StreamOutputChannel.OUTPUT_DISABLED, new IPCEventListener() {
      @Override
      public void triggered(final IPCEvent event) {
        taskResourceManager.getOwnerTask().notifyOutputDisabled(localizedOutputIDs[i]);
      }
    });
    ioChannels[i].addListener(StreamOutputChannel.OUTPUT_RECOVERED, new IPCEventListener() {
      @Override
      public void triggered(final IPCEvent event) {
        taskResourceManager.getOwnerTask().notifyOutputEnabled(localizedOutputIDs[i]);
      }
    });
    buffers[i] = new TupleBatchBuffer(getSchema());
    ioChannelsAvail[i] = true;
    backupBuffers.add(i, new ArrayList<TupleBatch>());
  }

  /**
   * @param chIdx the channel to write
   * @param msg the message.
   * @return write future
   * */
  protected final ChannelFuture writeMessage(final int chIdx, final TupleBatch msg) {
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
          try {
            Thread.sleep(toSleep);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
          }
          sleepTime *= 2;
        }
      }
    }
  }

  /**
   * Pop tuple batches from each of the buffers and try to write them to corresponding channels, if possible.
   * 
   * @param usingTimeout use popAny() or popAnyUsingTimeout() when poping
   * */
  protected final void popTBsFromBuffersAndWrite(final boolean usingTimeout) {
    popTBsFromBuffersAndWrite(usingTimeout, ArrayUtils.create2DIndex(numChannels()));
  }

  /**
   * Pop tuple batches from each of the buffers and try to write them to corresponding channels, if possible.
   * 
   * @param usingTimeout use popAny() or popAnyUsingTimeout() when poping
   * @param channelIndices the same as GenericShuffleProducer's cellPartition
   * */
  protected final void popTBsFromBuffersAndWrite(final boolean usingTimeout, final int[][] channelIndices) {
    final TupleBatchBuffer[] tbb = getBuffers();
    FTMODE mode = taskResourceManager.getOwnerTask().getOwnerQuery().getFTMode();
    for (int i = 0; i < numChannels(); i++) {
      while (true) {
        TupleBatch tb = null;
        if (usingTimeout) {
          tb = tbb[i].popAnyUsingTimeout();
        } else {
          tb = tbb[i].popAny();
        }
        if (tb == null) {
          break;
        }

        for (int j : channelIndices[i]) {
          if (mode.equals(FTMODE.rejoin)) {
            // rejoin, append the TB into the backup buffer in case of recovering
            backupBuffers.get(j).add(tb);
          }
          if (!ioChannelsAvail[j] && (mode.equals(FTMODE.abandon) || mode.equals(FTMODE.rejoin))) {
            continue;
          }
          try {
            writeMessage(j, tb);
          } catch (IllegalStateException e) {
            if (mode.equals(FTMODE.abandon)) {
              ioChannelsAvail[j] = false;
              break;
            } else if (mode.equals(FTMODE.rejoin)) {
              ioChannelsAvail[j] = false;
              break;
            } else {
              throw e;
            }
          }
        }
      }
    }
  }

  /**
   * @param chIdx the channel to write
   * @return channel release future.
   * */
  protected final ChannelFuture channelEnds(final int chIdx) {
    if (ioChannelsAvail[chIdx]) {
      return ioChannels[chIdx].release();
    }
    return null;
  }

  @Override
  public final void cleanup() throws DbException {
    for (int i = 0; i < localizedOutputIDs.length; i++) {
      buffers[i] = null;
      if (ioChannels[i] != null) {
        /* RecoverProducer may detach & set its channel to be null, shouldn't call release here */
        ioChannels[i].release();
      }
    }
    buffers = null;
  }

  /**
   * @param myWorkerID for parsing self-references.
   * @return destination worker IDs.
   * */
  public final StreamIOChannelID[] getOutputChannelIDs(final int myWorkerID) {
    StreamIOChannelID[] result = new StreamIOChannelID[outputIDs.length];
    int idx = 0;
    for (StreamIOChannelID ecID : outputIDs) {
      if (ecID.getRemoteID() != IPCConnectionPool.SELF_IPC_ID) {
        result[idx++] = ecID;
      } else {
        result[idx++] = new StreamIOChannelID(ecID.getStreamID(), myWorkerID);
      }
    }

    return result;
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

  /**
   * @return The resource manager of the running task.
   * */
  protected TaskResourceManager getTaskResourceManager() {
    return taskResourceManager;
  }

  /**
   * enable/disable output channels that belong to the worker.
   * 
   * @param workerId the worker that changed its status.
   * @param enable enable/disable all the channels that belong to the worker.
   * */
  public final void updateChannelAvailability(final int workerId, final boolean enable) {
    List<Integer> indices = getChannelIndicesOfAWorker(workerId);
    for (int i : indices) {
      ioChannelsAvail[i] = enable;
    }
  }

  /**
   * return the backup buffers.
   * 
   * @return backup buffers.
   */
  public final List<List<TupleBatch>> getBackupBuffers() {
    return backupBuffers;
  }

  /**
   * return the indices of the channels that belong to the worker.
   * 
   * @param workerId the id of the worker.
   * @return the list of channel indices.
   * */
  public final List<Integer> getChannelIndicesOfAWorker(final int workerId) {
    List<Integer> ret = new ArrayList<Integer>();
    for (int i = 0; i < numChannels(); ++i) {
      if (ioChannels[i].getID().getRemoteID() == workerId) {
        ret.add(i);
      }
    }
    return ret;
  }

  /**
   * @return the channel availability array.
   */
  public boolean[] getChannelsAvail() {
    return ioChannelsAvail;
  }

  /**
   * @return the channel array.
   */
  public StreamOutputChannel<TupleBatch>[] getChannels() {
    return ioChannels;
  }

  /**
   * process EOS and EOI logic.
   * */
  @Override
  protected final void checkEOSAndEOI() {
    Operator child = getChild();
    if (child.eoi()) {
      setEOI(true);
      child.setEOI(false);
    } else if (child.eos()) {
      if (taskResourceManager.getOwnerTask().getOwnerQuery().getFTMode().equals(FTMODE.rejoin)) {
        for (TupleBatchBuffer tbb : buffers) {
          if (tbb.numTuples() > 0) {
            // due to failure, buffers are not empty, this task needs to be executed again to push these TBs out when
            // channels are available
            return;
          }
        }
      }
      // all buffers are empty, ready to end this task
      setEOS();
    }
  }
}
