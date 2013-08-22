package edu.washington.escience.myria.parallel;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.parallel.ipc.IPCConnectionPool;
import edu.washington.escience.myria.parallel.ipc.StreamOutputChannel;

/**
 * Non-blocking driving code of a sub-query.
 * 
 * Task state could be:<br>
 * 1) In execution.<br>
 * 2) In dormant.<br>
 * 3) Already finished.<br>
 * 4) Has not started.
 * */
public final class TaskResourceManager {

  /** The logger for this class. */
  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TaskResourceManager.class.getName());

  /**
   * The ipc pool.
   * */
  private final IPCConnectionPool ipcPool;

  /**
   * All output channels owned by the owner task.
   * */
  private final Set<StreamOutputChannel<TupleBatch>> outputChannels;

  /**
   * owner task.
   * */
  private final QuerySubTreeTask ownerTask;

  /**
   * task execution mode.
   * */
  private final QueryExecutionMode executionMode;

  /**
   * @param connectionPool connection pool.
   * @param ownerTask owner task
   * @param executionMode the task execution mode.
   */
  TaskResourceManager(final IPCConnectionPool connectionPool, final QuerySubTreeTask ownerTask,
      final QueryExecutionMode executionMode) {
    ipcPool = connectionPool;
    outputChannels = Sets.newSetFromMap(new ConcurrentHashMap<StreamOutputChannel<TupleBatch>, Boolean>());
    this.ownerTask = ownerTask;
    this.executionMode = executionMode;
  }

  /**
   * start a data output stream.
   * 
   * @param remoteWorkerID remoteWorker
   * @param operatorID remote receive operator
   * @return a output channel.
   * */
  public StreamOutputChannel<TupleBatch> startAStream(final int remoteWorkerID, final ExchangePairID operatorID) {
    return this.startAStream(remoteWorkerID, operatorID.getLong());
  }

  /**
   * start a data output stream.
   * 
   * @param remoteWorkerID remoteWorker
   * @param streamID remote receive operator
   * @return a output channel.
   * */
  public StreamOutputChannel<TupleBatch> startAStream(final int remoteWorkerID, final long streamID) {
    StreamOutputChannel<TupleBatch> output = ipcPool.reserveLongTermConnection(remoteWorkerID, streamID);
    outputChannels.add(output);
    return output;
  }

  /**
   * remove an output channel from outputChannels. Need it when a recovery task is finished and needs to detach & attach
   * its channel to the original producer. In this case, the channel shouldn't be released when the cleanup() method of
   * the recovery task is called.
   * 
   * @param channel the channel to be removed.
   * */
  public void removeOutputChannel(final StreamOutputChannel<TupleBatch> channel) {
    outputChannels.remove(channel);
  }

  /**
   * clean up, release all resources.
   * */
  public void cleanup() {
    for (StreamOutputChannel<TupleBatch> out : outputChannels) {
      out.release();
    }
  }

  /**
   * @return the worker/master id where the task resides.
   * */
  public int getMyWorkerID() {
    return ipcPool.getMyIPCID();
  }

  /**
   * @return owner task.
   * */
  public QuerySubTreeTask getOwnerTask() {
    return ownerTask;
  }

  /**
   * @return execution mode
   * */
  public QueryExecutionMode getExecutionMode() {
    return executionMode;
  }

}
