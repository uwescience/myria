package edu.washington.escience.myria.operator.network;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.jboss.netty.channel.ChannelFuture;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.operator.DupElim;
import edu.washington.escience.myria.operator.KeepAndSortOnMinValue;
import edu.washington.escience.myria.operator.KeepMinValue;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.operator.SimpleAppender;
import edu.washington.escience.myria.operator.StreamingState;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.parallel.LocalFragmentResourceManager;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.parallel.ipc.StreamIOChannelID;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * A Producer is the counterpart of a consumer. It dispatch data using IPC channels to Consumers. Like network socket,
 * Each (workerID, operatorID) pair is a logical destination.
 * */
public abstract class Producer extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The worker this operator is located at.
   */
  private transient LocalFragmentResourceManager taskResourceManager;

  /**
   * the netty channels doing the true IPC IO.
   * */
  private transient StreamOutputChannel<TupleBatch>[] ioChannels;
  /**
   * if the corresponding ioChannel is available to write again.
   * */
  private transient boolean[] ioChannelsAvail;

  /**
   * output buffers of partitions.
   * */
  private transient TupleBatchBuffer[] partitionBuffers;

  /** tried to send tuples for each channel. */
  private List<StreamingState> triedToSendTuples;
  /** pending tuples to be sent for each channel. */
  private transient List<LinkedList<TupleBatch>> pendingTuplesToSend;

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

  /** number of parition, by default 1. */
  private int numOfPartition = 1;

  /** if the outgoing channels are totally local. */
  private boolean totallyLocal;

  /**
   * no worker means to the owner worker.
   * 
   * @param child the child providing data.
   * @param oIDs operator IDs.
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs) {
    this(child, oIDs, MyriaArrayUtils.arrayFillAndReturn(new int[oIDs.length], IPCConnectionPool.SELF_IPC_ID), true);
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
    this(child, (ExchangePairID[]) MyriaArrayUtils.arrayFillAndReturn(new ExchangePairID[destinationWorkerIDs.length],
        oID), destinationWorkerIDs, true);
  }

  /**
   * same worker with different oIDs (multiway copy).
   * 
   * @param oIDs the operator IDs.
   * @param child the child providing data.
   * @param destinationWorkerID the worker ID.
   * */
  public Producer(final Operator child, final ExchangePairID[] oIDs, final int destinationWorkerID) {
    this(child, oIDs, MyriaArrayUtils.arrayFillAndReturn(new int[oIDs.length], Integer.valueOf(destinationWorkerID)),
        true);
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
    totallyLocal = true;
    for (int i : destinationWorkerIDs) {
      if (i != IPCConnectionPool.SELF_IPC_ID) {
        totallyLocal = false;
        break;
      }
    }
    // the default choice.
    setBackupBufferAsAppender();
  }

  /**
   * @return the outputIDs
   */
  protected StreamIOChannelID[] getOutputIDs() {
    return outputIDs;
  }

  @SuppressWarnings("unchecked")
  @Override
  public final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    taskResourceManager =
        (LocalFragmentResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_FRAGMENT_RESOURCE_MANAGER);
    partitionBuffers = new TupleBatchBuffer[numOfPartition];
    for (int i = 0; i < numOfPartition; i++) {
      partitionBuffers[i] = new TupleBatchBuffer(getSchema());
    }
    ioChannels = new StreamOutputChannel[outputIDs.length];
    ioChannelsAvail = new boolean[outputIDs.length];
    pendingTuplesToSend = new ArrayList<LinkedList<TupleBatch>>();
    localizedOutputIDs = new StreamIOChannelID[outputIDs.length];
    for (int i = 0; i < outputIDs.length; i++) {
      if (outputIDs[i].getRemoteID() == IPCConnectionPool.SELF_IPC_ID) {
        localizedOutputIDs[i] = new StreamIOChannelID(outputIDs[i].getStreamID(), taskResourceManager.getNodeId());
      } else {
        localizedOutputIDs[i] = outputIDs[i];
      }
    }
    for (int i = 0; i < localizedOutputIDs.length; i++) {
      createANewChannel(i);
      pendingTuplesToSend.add(i, new LinkedList<TupleBatch>());
      triedToSendTuples.get(i).init(execEnvVars);
    }
    nonBlockingExecution =
        (execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE) == QueryExecutionMode.NON_BLOCKING);
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
        taskResourceManager.getFragment().notifyOutputDisabled(localizedOutputIDs[i]);
      }
    });
    ioChannels[i].addListener(StreamOutputChannel.OUTPUT_RECOVERED, new IPCEventListener() {
      @Override
      public void triggered(final IPCEvent event) {
        taskResourceManager.getFragment().notifyOutputEnabled(localizedOutputIDs[i]);
      }
    });
    ioChannelsAvail[i] = true;
  }

  /**
   * set backup buffers as KeepMinValue.
   * 
   * @param keyColIndices the same as the one in KeepMinValue
   * @param valueColIndices the same as the one in KeepMinValue
   */
  public void setBackupBufferAsMin(final int[] keyColIndices, final int[] valueColIndices) {
    triedToSendTuples = new ArrayList<StreamingState>();
    for (int i = 0; i < outputIDs.length; i++) {
      triedToSendTuples.add(i, new KeepMinValue(keyColIndices, valueColIndices));
      triedToSendTuples.get(i).setAttachedOperator(this);
    }
  }

  /**
   * set backup buffers as KeepAndSortOnMinValue.
   * 
   * @param keyColIndices the same as the one in KeepAndSortOnMinValue
   * @param valueColIndices the same as the one in KeepAndSortOnMinValue
   */
  public void setBackupBufferAsPrioritizedMin(final int[] keyColIndices, final int[] valueColIndices) {
    triedToSendTuples = new ArrayList<StreamingState>();
    for (int i = 0; i < outputIDs.length; i++) {
      triedToSendTuples.add(i, new KeepAndSortOnMinValue(keyColIndices, valueColIndices));
      triedToSendTuples.get(i).setAttachedOperator(this);
    }
  }

  /** set backup buffers as DupElim. */
  public void setBackupBufferAsDupElim() {
    triedToSendTuples = new ArrayList<StreamingState>();
    for (int i = 0; i < outputIDs.length; i++) {
      triedToSendTuples.add(i, new DupElim());
      triedToSendTuples.get(i).setAttachedOperator(this);
    }
  }

  /** set backup buffers as SimpleAppender. */
  public void setBackupBufferAsAppender() {
    triedToSendTuples = new ArrayList<StreamingState>();
    for (int i = 0; i < outputIDs.length; i++) {
      triedToSendTuples.add(i, new SimpleAppender());
      triedToSendTuples.get(i).setAttachedOperator(this);
    }
  }

  /** the number of tuples written to channels. */
  private long numTuplesWrittenToChannels = 0;

  /**
   * @param chIdx the channel to write
   * @param msg the message.
   * @return write future
   * */
  protected final ChannelFuture writeMessage(final int chIdx, final TupleBatch msg) {
    StreamOutputChannel<TupleBatch> ch = ioChannels[chIdx];
    if (nonBlockingExecution) {
      numTuplesWrittenToChannels += msg.numTuples();
      return ch.write(msg);
    } else {
      int sleepTime = 1;
      int maxSleepTime = MyriaConstants.SHORT_WAITING_INTERVAL_MS;
      while (true) {
        if (ch.isWritable()) {
          numTuplesWrittenToChannels += msg.numTuples();
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
   * @param partitions the list of partitions as tuple batches.
   * */
  protected final void writePartitionsIntoChannels(final boolean usingTimeout, final TupleBatch[] partitions) {
    writePartitionsIntoChannels(usingTimeout, MyriaArrayUtils.create2DVerticalIndex(numChannels()), partitions);
  }

  /**
   * Pop tuple batches from each of the buffers and try to write them to corresponding channels, if possible.
   * 
   * @param usingTimeout use {@link TupleBatchBuffer#popAny()} or {@link TupleBatchBuffer#popAnyUsingTimeout()} when
   *          popping
   * @param channelIndices the same as {@link GenericShuffleProducer#cellPartition}.
   * @param partitions the list of partitions as tuple batches.
   * */
  protected final void writePartitionsIntoChannels(final boolean usingTimeout, final int[][] channelIndices,
      final TupleBatch[] partitions) {
    FTMode mode = taskResourceManager.getFragment().getLocalSubQuery().getFTMode();

    if (totallyLocal) {
      if (partitions != null) {
        for (int i = 0; i < numOfPartition; ++i) {
          if (partitions[i] != null) {
            for (int j : channelIndices[i]) {
              if (!ioChannelsAvail[j] && mode.equals(FTMode.ABANDON)) {
                continue;
              }
              pendingTuplesToSend.get(j).add(partitions[i]);
            }
          }
        }
      }
    } else {
      if (partitions != null) {
        for (int i = 0; i < numOfPartition; ++i) {
          if (partitions[i] != null) {
            partitionBuffers[i].absorb(partitions[i]);
          }
        }
      }
      for (int i = 0; i < numOfPartition; ++i) {
        while (true) {
          TupleBatch tb = null;
          if (usingTimeout) {
            tb = partitionBuffers[i].popAnyUsingTimeout();
          } else {
            tb = partitionBuffers[i].popAny();
          }
          if (tb == null) {
            break;
          }
          for (int j : channelIndices[i]) {
            if (!ioChannelsAvail[j] && mode.equals(FTMode.ABANDON)) {
              continue;
            }
            pendingTuplesToSend.get(j).add(tb);
          }
        }
      }
    }

    for (int i = 0; i < numChannels(); ++i) {
      if (!ioChannelsAvail[i] && (mode.equals(FTMode.ABANDON) || mode.equals(FTMode.REJOIN))) {
        continue;
      }
      while (true) {
        TupleBatch tb = pendingTuplesToSend.get(i).poll();
        if (tb == null) {
          break;
        }
        if (mode.equals(FTMode.REJOIN) && !(this instanceof LocalMultiwayProducer)) {
          // rejoin, append the TB into the backup buffer in case of recovering
          tb = triedToSendTuples.get(i).update(tb);
        }
        try {
          if (tb != null) {
            writeMessage(i, tb);
          }
        } catch (IllegalStateException e) {
          if (mode.equals(FTMode.ABANDON)) {
            ioChannelsAvail[i] = false;
            break;
          } else if (mode.equals(FTMode.REJOIN)) {
            ioChannelsAvail[i] = false;
            break;
          } else {
            throw e;
          }
        }
      }
    }
  }

  /**
   * @return the number of tuples in all buffers.
   */
  public final long getNumTuplesInBuffers() {
    long sum = 0;
    for (StreamingState state : triedToSendTuples) {
      sum += state.numTuples();
    }
    return sum;
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
      if (ioChannels[i] != null) {
        /* RecoverProducer may detach & set its channel to be null, shouldn't call release here */
        ioChannels[i].release();
      }
    }
    for (int i = 0; i < numOfPartition; i++) {
      partitionBuffers[i] = null;
    }
    partitionBuffers = null;
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
   * @return The resource manager of the running task.
   * */
  protected LocalFragmentResourceManager getTaskResourceManager() {
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
  public final List<StreamingState> getTriedToSendTuples() {
    return triedToSendTuples;
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
      if (taskResourceManager.getFragment().getLocalSubQuery().getFTMode().equals(FTMode.REJOIN)) {
        for (LinkedList<TupleBatch> tbs : pendingTuplesToSend) {
          if (tbs.size() > 0) {
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

  /**
   * set the number of partitions.
   * 
   * @param num the number
   */
  public void setNumOfPartition(final int num) {
    numOfPartition = num;
  }

  /**
   * 
   * @return the number of partitions.
   */
  public int getNumOfPartition() {
    return numOfPartition;
  }

  /**
   * @return the number of tuples written to channels.
   */
  public final long getNumTuplesWrittenToChannels() {
    return numTuplesWrittenToChannels;
  }
}
