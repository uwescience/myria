package edu.washington.escience.myria.parallel;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;

import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Holds the IPC resources for a specific fragment.
 */
public final class LocalFragmentResourceManager {

  /** The logger for this class. */
  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(LocalFragmentResourceManager.class);

  /**
   * The ipc pool.
   */
  private final IPCConnectionPool ipcPool;

  /**
   * All output channels owned by this fragment.
   */
  private final Set<StreamOutputChannel<TupleBatch>> outputChannels;

  /** The corresponding fragment. */
  private final LocalFragment fragment;

  /**
   * @param connectionPool connection pool.
   * @param fragment the corresponding fragment
   */
  public LocalFragmentResourceManager(final IPCConnectionPool connectionPool, final LocalFragment fragment) {
    ipcPool = connectionPool;
    outputChannels = Sets.newSetFromMap(new ConcurrentHashMap<StreamOutputChannel<TupleBatch>, Boolean>());
    this.fragment = fragment;
  }

  /**
   * start a data output stream.
   * 
   * @param remoteWorkerID remoteWorker
   * @param operatorID remote receive operator
   * @return a output channel.
   */
  public StreamOutputChannel<TupleBatch> startAStream(final int remoteWorkerID, final ExchangePairID operatorID) {
    return this.startAStream(remoteWorkerID, operatorID.getLong());
  }

  /**
   * start a data output stream.
   * 
   * @param remoteWorkerID remoteWorker
   * @param streamID remote receive operator
   * @return a output channel.
   */
  public StreamOutputChannel<TupleBatch> startAStream(final int remoteWorkerID, final long streamID) {
    StreamOutputChannel<TupleBatch> output = ipcPool.reserveLongTermConnection(remoteWorkerID, streamID);
    outputChannels.add(output);
    return output;
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
