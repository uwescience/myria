package edu.washington.escience.myria.parallel;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Verify;
import com.google.common.collect.Sets;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.ipc.FlowControlBagInputBuffer;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.IPCEvent;
import edu.washington.escience.myria.parallel.ipc.IPCEventListener;
import edu.washington.escience.myria.parallel.ipc.StreamInputBuffer;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Holds the IPC resources for a specific fragment.
 */
public final class LocalFragmentResourceManager {

  /** The logger for this class. */
  static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(LocalFragmentResourceManager.class);

  /**
   * The ipc pool.
   */
  private final IPCConnectionPool ipcPool;

  /**
   * All output channels owned by this fragment.
   */
  private final Set<StreamOutputChannel<TupleBatch>> outputChannels;

  /**
   * All input buffers owned by this fragment.
   */
  private final ConcurrentHashMap<Consumer, StreamInputBuffer<TupleBatch>> inputBuffers;

  /** The corresponding fragment. */
  private final LocalFragment fragment;

  /**
   * @param connectionPool connection pool.
   * @param fragment the corresponding fragment
   */
  public LocalFragmentResourceManager(
      final IPCConnectionPool connectionPool, final LocalFragment fragment) {
    inputBuffers = new ConcurrentHashMap<Consumer, StreamInputBuffer<TupleBatch>>();
    ipcPool = connectionPool;
    outputChannels =
        Sets.newSetFromMap(new ConcurrentHashMap<StreamOutputChannel<TupleBatch>, Boolean>());

    this.fragment = fragment;
  }

  /**
   * start a data output stream.
   *
   * @param remoteWorkerID remoteWorker
   * @param operatorID remote receive operator
   * @return a output channel.
   */
  public StreamOutputChannel<TupleBatch> startAStream(
      final int remoteWorkerID, final ExchangePairID operatorID) {
    return this.startAStream(remoteWorkerID, operatorID.getLong());
  }

  /**
   * start a data output stream.
   *
   * @param remoteWorkerID remoteWorker
   * @param streamID remote receive operator
   * @return a output channel.
   */
  public StreamOutputChannel<TupleBatch> startAStream(
      final int remoteWorkerID, final long streamID) {
    StreamOutputChannel<TupleBatch> output =
        ipcPool.reserveLongTermConnection(remoteWorkerID, streamID);
    outputChannels.add(output);
    return output;
  }

  /**
   * Release input buffers.
   *
   * @param consumer the owner of the input buffer.
   */
  public void releaseInputBuffer(final Consumer consumer) {
    StreamInputBuffer<TupleBatch> input = inputBuffers.remove(consumer);
    if (input != null) {
      input.clear();
      input.getOwnerConnectionPool().deRegisterStreamInput(input);
    }
  }

  /**
   * Allocate input buffers.
   *
   * @param consumer the owner of the input buffer.
   * @return the allocated input buffer.
   * */
  public StreamInputBuffer<TupleBatch> getInputBuffer(final Consumer consumer) {
    return Verify.verifyNotNull(inputBuffers.get(consumer));
  }

  /**
   * Allocate input buffers.
   *
   * @param consumer the owner of the input buffer.
   * @return the allocated input buffer.
   * */
  public StreamInputBuffer<TupleBatch> allocateInputBuffer(final Consumer consumer) {
    StreamInputBuffer<TupleBatch> inputBuffer = inputBuffers.get(consumer);

    if (inputBuffer != null) {
      return inputBuffer;
    }

    inputBuffer =
        new FlowControlBagInputBuffer<TupleBatch>(
            ipcPool,
            consumer.getInputChannelIDs(ipcPool.getMyIPCID()),
            ipcPool.getInputBufferCapacity(),
            ipcPool.getInputBufferRecoverTrigger());
    inputBuffer.addListener(
        FlowControlBagInputBuffer.NEW_INPUT_DATA,
        new IPCEventListener() {

          @Override
          public void triggered(final IPCEvent event) {
            fragment.notifyNewInput();
          }
        });

    inputBuffers.put(consumer, inputBuffer);
    inputBuffer.setAttachment(consumer.getSchema());
    inputBuffer.start(consumer);
    return inputBuffer;
  }

  /**
   * Remove an output channel from outputChannels. Need it when a recovery task is finished and needs to detach & attach
   * its channel to the original producer. In this case, the channel shouldn't be released when the cleanup() method of
   * the recovery task is called.
   *
   * @param channel the channel to be removed.
   */
  public void removeOutputChannel(final StreamOutputChannel<TupleBatch> channel) {
    outputChannels.remove(channel);
  }

  /**
   * clean up, release all resources.
   */
  public void cleanup() {
    for (StreamOutputChannel<TupleBatch> out : outputChannels) {
      out.release();
    }
    outputChannels.clear();
    for (Consumer c : inputBuffers.keySet()) {
      releaseInputBuffer(c);
    }
    inputBuffers.clear();
  }

  /**
   * @return the ID of the node (worker or master) on which this fragment is executing.
   */
  public int getNodeId() {
    return ipcPool.getMyIPCID();
  }

  /**
   * @return the corresponding fragment.
   */
  public LocalFragment getFragment() {
    return fragment;
  }
}
