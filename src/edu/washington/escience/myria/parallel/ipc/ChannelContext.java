package edu.washington.escience.myria.parallel.ipc;

import java.nio.channels.ClosedChannelException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.parallel.ipc.IPCMessage.Meta.CONNECT;
import edu.washington.escience.myria.util.AttachmentableAdapter;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.concurrent.DefaultOperationFuture;
import edu.washington.escience.myria.util.concurrent.OperationFuture;
import edu.washington.escience.myria.util.concurrent.OperationFutureListener;
import edu.washington.escience.myria.util.concurrent.ThreadStackDump;

/**
 * Recording the various context information of a channel. The most important part of this class is the state machine of
 * a channel.
 * */
public class ChannelContext extends AttachmentableAdapter {

  /**
   * Channel close requested event.
   * */
  class ChannelCloseRequested implements DelayedTransitionEvent {
    /**
     * the channel pool in which the channel resides.
     * */
    private final ChannelPrioritySet channelPool;

    /**
     * channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels reside.
     * */
    private final ConcurrentHashMap<Channel, Channel> recycleBin;

    /**
     * channel trash bin. The place where to-be-closed channels reside.
     * */
    private final ChannelGroup trashBin;

    /**
     * @param channelPool the channel pool in which the channel resides.
     * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
     *          reside.
     * @param trashBin channel trash bin. The place where to-be-closed channels reside.
     * */
    ChannelCloseRequested(
        final ChannelPrioritySet channelPool,
        final ConcurrentHashMap<Channel, Channel> recycleBin,
        final ChannelGroup trashBin) {
      this.channelPool = channelPool;
      this.recycleBin = recycleBin;
      this.trashBin = trashBin;
    }

    @Override
    public final boolean apply() {
      // accepted channel
      synchronized (stateMachineLock) {
        if (connected
                && !registered
                && !inPool
                && !inRecycleBin
                && !inTrashBin
                && newConnection
                && !closeRequested
            || connected
                && registered
                && inPool
                && !inRecycleBin
                && !inTrashBin
                && !newConnection
                && !closeRequested
            || connected
                && registered
                && inPool
                && inRecycleBin
                && !inTrashBin
                && !newConnection
                && !closeRequested
            || connected
                && registered
                && !inPool
                && !inRecycleBin
                && inTrashBin
                && !newConnection
                && !closeRequested) {
          if (!registered) {
            LOGGER.debug("Close at no registered");
            ownerChannel.disconnect();
            connected = false;
            closeRequested = true;
            newConnection = false;
          } else {
            inPool = false;
            inRecycleBin = false;
            inTrashBin = true;
            newConnection = false;
            closeRequested = true;
            if (channelPool != null) {
              channelPool.remove(ownerChannel);
            }
            recycleBin.remove(ownerChannel);
            trashBin.add(ownerChannel);
          }
        } else {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Delayed event.
   * */
  interface DelayedTransitionEvent {
    /**
     * apply the event.
     *
     * @return if successfully applied.
     * */
    boolean apply();
  }

  /**
   * ID checking timeout event.
   * */
  class IDCheckingTimeout implements DelayedTransitionEvent {
    /**
     * Set of new channels who have not identified there worker IDs yet (i.e. not registered).
     * */
    private final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels;

    /**
     * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
     *          registered).
     * */
    IDCheckingTimeout(final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
      this.unregisteredNewChannels = unregisteredNewChannels;
    }

    @Override
    public final boolean apply() {
      if (ownerChannel.getParent() != null) {
        // accepted channel
        synchronized (stateMachineLock) {
          if ((!connected
                  && !registered
                  && !inPool
                  && !inRecycleBin
                  && !inTrashBin
                  && newConnection
                  && !closeRequested)
              || (connected
                  && !registered
                  && !inPool
                  && !inRecycleBin
                  && !inTrashBin
                  && newConnection
                  && !closeRequested)) {
            newConnection = false;
            if (connected) {
              ownerChannel.disconnect();
            }
            connected = false;
            unregisteredNewChannels.remove(ownerChannel);
          } else {
            return false;
          }
        }
      } else {
        // client channel
        synchronized (stateMachineLock) {
          if ((!connected
                  && !registered
                  && !inPool
                  && !inRecycleBin
                  && !inTrashBin
                  && newConnection)
              || (connected
                  && !registered
                  && !inPool
                  && !inRecycleBin
                  && !inTrashBin
                  && newConnection)) {
            newConnection = false;
            if (connected) {
              ownerChannel.disconnect();
            }
            connected = false;
            unregisteredNewChannels.remove(ownerChannel);
          } else {
            return false;
          }
        }
      }
      return false;
    }
  }

  /**
   * IPC remote get removed event.
   * */
  final class IPCRemoteRemoved implements DelayedTransitionEvent {
    /**
     * channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels reside.
     * */
    private final ConcurrentHashMap<Channel, Channel> recycleBin;
    /**
     * channel trash bin. The place where to-be-closed channels reside.
     * */
    private final ChannelGroup trashBin;
    /**
     * the channel pool in which the channel resides.
     * */
    private final ChannelPrioritySet channelPool;

    /**
     * @param channelPool the channel pool in which the channel resides.
     * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
     *          reside.
     * @param trashBin channel trash bin. The place where to-be-closed channels reside.
     * */
    IPCRemoteRemoved(
        final ConcurrentHashMap<Channel, Channel> recycleBin,
        final ChannelGroup trashBin,
        final ChannelPrioritySet channelPool) {
      this.recycleBin = recycleBin;
      this.trashBin = trashBin;
      this.channelPool = channelPool;
    }

    @Override
    public boolean apply() {
      if (channelPool != null) {
        channelPool.getUpdateLock().lock();
      }

      try {
        if (ownerChannel.getParent() != null) {
          // accepted channel
          synchronized (stateMachineLock) {
            if ((connected
                    && registered
                    && inPool
                    && inRecycleBin
                    && !inTrashBin
                    && !newConnection
                    && !closeRequested)
                || (connected
                    && registered
                    && inPool
                    && !inRecycleBin
                    && !inTrashBin
                    && !newConnection
                    && !closeRequested)) {
              inRecycleBin = false;
              inTrashBin = true;
              inPool = false;
              recycleBin.remove(ownerChannel);
              trashBin.add(ownerChannel);
              if (channelPool != null) {
                channelPool.remove(ownerChannel);
              }
            } else {
              return false;
            }
          }
        } else {
          // client channel
          synchronized (stateMachineLock) {
            if ((connected && registered && inPool && inRecycleBin && !inTrashBin && !newConnection)
                || (connected
                    && registered
                    && inPool
                    && !inRecycleBin
                    && !inTrashBin
                    && !newConnection)) {
              inRecycleBin = false;
              inTrashBin = true;
              inPool = false;
              recycleBin.remove(ownerChannel);
              trashBin.add(ownerChannel);
              if (channelPool != null) {
                channelPool.remove(ownerChannel);
              }
            } else {
              return false;
            }
          }
        }
      } finally {
        if (channelPool != null) {
          channelPool.getUpdateLock().unlock();
        }
      }
      return true;
    }
  }

  /**
   * Extra state data of a registered channel.
   * */
  class RegisteredChannelContext {

    /**
     * remote id.
     * */
    private final int remoteID;

    /**
     * Which group the owner Channel belongs.
     * */
    private final ChannelPrioritySet channelGroup;

    /**
     * number of references.
     * */
    private volatile int numberOfReference;

    /**
     * The logical I/O pair of a physical channel.
     * */
    private final StreamIOChannelPair ioPair;

    /**
     * @param remoteID the remote IPC entity ID.
     * @param ownerChannelGroup which channel set the channel belongs.
     * */
    RegisteredChannelContext(final int remoteID, final ChannelPrioritySet ownerChannelGroup) {
      this.remoteID = remoteID;
      channelGroup = ownerChannelGroup;
      numberOfReference = 0;
      ioPair = new StreamIOChannelPair(ChannelContext.this);
    }

    /**
     * @return attachment.
     * */
    final StreamIOChannelPair getIOPair() {
      return ioPair;
    }

    /**
     * Decrease the number of references by 1.
     *
     * @return the new number of references.
     * */
    final int decReference() {
      int newRef = 0;
      synchronized (stateMachineLock) {
        if (numberOfReference <= 0) {
          newRef = -1;
          numberOfReference = 0;
        } else {
          int tmp = numberOfReference;
          numberOfReference = tmp - 1;
          newRef = numberOfReference;
        }
      }
      if (newRef < 0) {
        final String msg =
            "Number of references is negative, channel: "
                + ChannelContext.channelToString(ownerChannel);
        LOGGER.warn(msg, new ThreadStackDump());
        return 0;
      }
      return newRef;
    }

    /**
     * Clear the reference.
     * */
    final void clearReference() {
      synchronized (stateMachineLock) {
        numberOfReference = 0;
      }
    }

    /**
     * @return the remote IPC entity ID.
     * */
    final int getRemoteID() {
      return remoteID;
    }

    /**
     * Increase the number of references by 1.
     *
     * @return the new number of references.
     * */
    final int incReference() {
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "Inc reference for channel: {}",
            ChannelContext.channelToString(ownerChannel),
            new ThreadStackDump());
      }
      int newRef = 0;
      synchronized (stateMachineLock) {
        if (numberOfReference < 0) {
          newRef = -1;
          numberOfReference = 1;
        } else {
          int tmp = numberOfReference;
          numberOfReference = tmp + 1;
          newRef = numberOfReference;
        }
      }
      if (newRef < 0) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Number of references is negative, channel: {}",
              ChannelContext.channelToString(ownerChannel),
              new ThreadStackDump());
        }
        return 1;
      }
      return newRef;
    }

    /**
     * @return current number of references.
     * */
    final int numReferenced() {
      return numberOfReference;
    }

    /**
     * @return If the owner channel group get reordered
     * */
    final boolean updateLastIOTimestamp() {
      if (channelGroup == null) {
        return true;
      }
      if (!channelGroup.update(getChannel())) {
        // this channel has been deleted from
        // the channel group
        return false;
      }
      return true;
    }
  }

  /**
   * The server side of a channel is disconnected.
   * */
  class ServerSideDisconnect implements DelayedTransitionEvent {

    @Override
    public final boolean apply() {
      // client channel
      synchronized (stateMachineLock) {
        if (connected && !registered && !inPool && !inRecycleBin && inTrashBin && !newConnection) {
          connected = false;
          inTrashBin = false;
        } else {
          return false;
        }
      }
      return true;
    }
  }

  /** The logger for this class. */
  protected static final Logger LOGGER = LoggerFactory.getLogger(ChannelContext.class);

  /**
   * @return the channel context data structure of the given channel.
   * @param channel the owner channel
   * */
  public static final ChannelContext getChannelContext(final Channel channel) {
    return (ChannelContext) channel.getAttachment();
  }

  /**
   * The owner channel of this ChannelContext, i.e. ownerChannel.getAttachment() == this.
   * */
  private final Channel ownerChannel;

  /**
   * remote id.
   * */
  private final int myID;

  /**
   * The most recent write future.
   * */
  private volatile ChannelFuture mostRecentWriteFuture = null;

  /**
   * the set of futures waiting for the channel to get registered.
   * */
  private final HashSet<EqualityCloseFuture<Integer>> registerConditionFutures;

  /**
   * store the remote replied ID.
   * */
  private volatile Integer remoteReplyID = null;

  /**
   * For channels initiated by this IPC entity, the registration process is that this IPC entity creates a connection,
   * send my IPC ID, and wait for the remote IPC entity sending back its IPC ID within a timeout.
   * */
  private final DefaultOperationFuture remoteReply;

  /**
   * last IO timestamp. 0 or negative means the channel is connected by not used. If the channel is assigned to do some
   * IO task, then this field must be updated to the timestamp of assignment. Also, each IO operation on this channel
   * should update this timestamp.
   * */
  private volatile long lastIOTimestamp;

  /**
   * synchronize channel state change. The channel state machine diagram is in ipc_pool_channel_statemachine.di which
   * can be open by the Papyrus Eclipse plugin.
   *
   */
  private final Object stateMachineLock;
  /**
   * Extra context data structure if the owner channel successfully registered.
   * */
  private volatile RegisteredChannelContext registeredContext;
  /**
   * Binary state variable, channel is connected.
   * */
  private boolean connected = false;
  /**
   * Binary state variable, channel is registered.
   * */
  private boolean registered = false;
  /**
   * Binary state variable, channel is in the ipc pool.
   * */
  private volatile boolean inPool = false;
  /**
   * Binary state variable, channel is in recycle bin (will be moved to trash bin if time out.).
   * */
  private boolean inRecycleBin = false;

  /**
   * Binary state variable, channel is in trash bin (will never be used, disconnect if properly).
   * */
  private boolean inTrashBin = false;

  /**
   * Binary state variable, channel is newly created.
   * */
  private boolean newConnection = true;

  /**
   * Binary state variable, if it's a server accepted channel, denoting if the client side has requested closing it.
   * */
  private volatile boolean closeRequested = false;

  /**
   * delayed events (channel state is not yet at the state in which the events are applicable, but the events should not
   * be ignored. Apply them later if the channel state has changed.).
   * */
  private final ConcurrentLinkedQueue<DelayedTransitionEvent> delayedEvents;

  /**
   * @param channel the owner channel
   * @param ownerIPCID the owner {@link IPCConnectionPool}'s id.
   * */
  ChannelContext(final Channel channel, final int ownerIPCID) {
    lastIOTimestamp = System.currentTimeMillis();
    ownerChannel = channel;
    closeRequested = false;
    stateMachineLock = new Object();
    registeredContext = null;
    registerConditionFutures = new HashSet<EqualityCloseFuture<Integer>>();
    delayedEvents = new ConcurrentLinkedQueue<DelayedTransitionEvent>();
    remoteReply = new DefaultOperationFuture(false);
    myID = ownerIPCID;
  }

  /**
   * @param future the future for channel registration
   * */
  final void addConditionFuture(final EqualityCloseFuture<Integer> future) {
    boolean registeredLocal;
    synchronized (stateMachineLock) {
      registeredLocal = registered;
      if (!registered) {
        registerConditionFutures.add(future);
      }
    }
    if (registeredLocal) {
      future.setActual(registeredContext.getRemoteID());
    }
  }

  /**
   * If there are delayed events queued, apply them now.
   * */
  private void applyDelayedTransitionEvents() {
    final Iterator<DelayedTransitionEvent> it = delayedEvents.iterator();
    while (it.hasNext()) {
      final DelayedTransitionEvent e = it.next();
      if (e.apply()) {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(e.getClass().getCanonicalName() + " delayed applied");
        }
        it.remove();
      }
    }
  }

  /**
   * @param channelPool the channel pool in which the channel resides.
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * */
  final void closeRequested(
      final ChannelPrioritySet channelPool,
      final ConcurrentHashMap<Channel, Channel> recycleBin,
      final ChannelGroup trashBin) {
    assert ownerChannel.getParent() != null;
    applyDelayedTransitionEvents();
    final ChannelCloseRequested ccr = new ChannelCloseRequested(channelPool, recycleBin, trashBin);
    if (!ccr.apply()) {
      delayedEvents.add(ccr);
    }
  }

  /**
   * Any error encountered, cleanup state, close the channel directly.
   *
   * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
   *          registered).
   *
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * @param channelPool the channel pool in which the channel resides. may be null if the channel is not registered yet
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * @param cause the error
   */
  final void errorEncountered(
      final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels,
      final ConcurrentHashMap<Channel, Channel> recycleBin,
      final ChannelGroup trashBin,
      final ChannelPrioritySet channelPool,
      final Throwable cause) {
    if (channelPool != null) {
      channelPool.getUpdateLock().lock();
    }

    try {
      synchronized (stateMachineLock) {
        if (connected) {
          unregisteredNewChannels.remove(ownerChannel);
          if (channelPool != null) {
            channelPool.remove(ownerChannel);
          }
          recycleBin.remove(ownerChannel);
          trashBin.remove(ownerChannel);
          connected = false;
          registered = false;
          inPool = false;
          inRecycleBin = false;
          inTrashBin = false;
          newConnection = false;
          closeRequested = false;
          if (ownerChannel.getParent() == null) {
            remoteReply.setFailure(cause);
          }
        }
      }
    } finally {
      if (channelPool != null) {
        channelPool.getUpdateLock().unlock();
      }
    }
    ownerChannel.close();
  }

  /**
   * Callback if the owner channel is closed.
   *
   * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
   *          registered).
   *
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * @param channelPool the channel pool in which the channel resides. may be null if the channel is not registered yet
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   */
  final void closed(
      final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels,
      final ConcurrentHashMap<Channel, Channel> recycleBin,
      final ChannelGroup trashBin,
      final ChannelPrioritySet channelPool) {
    if (channelPool != null) {
      channelPool.getUpdateLock().lock();
    }

    try {

      synchronized (stateMachineLock) {
        if (connected) {
          // it's an abnormal disconnect
          RegisteredChannelContext rcc = getRegisteredChannelContext();
          if (rcc != null) {
            // clear the number of reference
            rcc.clearReference();
          }
          connected = false;

          unregisteredNewChannels.remove(ownerChannel);
          if (channelPool != null) {
            channelPool.remove(ownerChannel);
          }
          recycleBin.remove(ownerChannel);
          trashBin.remove(ownerChannel);
          connected = false;
          registered = false;
          inPool = false;
          inRecycleBin = false;
          inTrashBin = false;
          newConnection = false;
          closeRequested = false;

          if (ownerChannel.getParent() == null) {
            remoteReply.setFailure(
                new ChannelException(
                    "Channel " + channelToString(ownerChannel) + " closed",
                    new ClosedChannelException()));
          }
        }
      }
    } finally {
      if (channelPool != null) {
        channelPool.getUpdateLock().unlock();
      }
    }
  }

  /**
   * Callback when the owner channel is connected.
   * */
  final void connected() {
    applyDelayedTransitionEvents();
    // undelayed, must apply
    if (ownerChannel.getParent() != null) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert !connected
            && !registered
            && !inPool
            && !inRecycleBin
            && !inTrashBin
            && newConnection
            && !closeRequested;
        connected = true;
      }
    } else {
      // client channel
      synchronized (stateMachineLock) {
        assert !connected
            && !registered
            && !inPool
            && !inRecycleBin
            && !inTrashBin
            && newConnection;
        connected = true;
      }
    }
  }

  /**
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   *
   * */
  final void considerRecycle(final ConcurrentHashMap<Channel, Channel> recycleBin) {
    // undelayed, may not apply
    applyDelayedTransitionEvents();
    if (ownerChannel.getParent() != null) {
      // accepted channel
      synchronized (stateMachineLock) {
        if (connected
            && registered
            && inPool
            && !inRecycleBin
            && !inTrashBin
            && !newConnection
            && !closeRequested) {
          inRecycleBin = true;
          recycleBin.put(ownerChannel, ownerChannel);
        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "consider recycle unsatisfied: " + ChannelContext.channelToString(ownerChannel));
          }
        }
      }
    } else {
      // client channel
      synchronized (stateMachineLock) {
        if (connected && registered && inPool && !inRecycleBin && !inTrashBin && !newConnection) {
          inRecycleBin = true;
          recycleBin.put(ownerChannel, ownerChannel);
        } else {
          LOGGER.debug(
              "consider recycle unsatisfied: " + ChannelContext.channelToString(ownerChannel));
        }
      }
    }
  }

  /**
   * Callback when the owner channel has sent disconnect request to the remote part.
   * */
  final void disconnectSent() {
    // undelayed, must apply
    assert ownerChannel.getParent() == null;
    applyDelayedTransitionEvents();
    // client channel
    synchronized (stateMachineLock) {
      assert (connected && registered && !inPool && !inRecycleBin && inTrashBin && !newConnection);
      registered = false;
    }
  }

  /**
   * @return my owner channel.
   * */
  final Channel getChannel() {
    return ownerChannel;
  }

  /**
   * @return the timestamp of last IO operation.
   * */
  final long getLastIOTimestamp() {
    return lastIOTimestamp;
  }

  /**
   * @return the write future of the most recent write action.
   * */
  final ChannelFuture getMostRecentWriteFuture() {
    return mostRecentWriteFuture;
  }

  /**
   * @return the extra channel context data structure if the channel is registered.
   * */
  final RegisteredChannelContext getRegisteredChannelContext() {
    return registeredContext;
  }

  /**
   * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
   *          registered).
   * */
  final void idCheckingTimeout(final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
    applyDelayedTransitionEvents();
    final IDCheckingTimeout idct = new IDCheckingTimeout(unregisteredNewChannels);
    if (!idct.apply()) {
      delayedEvents.add(idct);
    }
  }

  /**
   * @param channelPool the channel pool in which the channel resides.
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * */
  final void ipcRemoteRemoved(
      final ConcurrentHashMap<Channel, Channel> recycleBin,
      final ChannelGroup trashBin,
      final ChannelPrioritySet channelPool) {
    applyDelayedTransitionEvents();
    final IPCRemoteRemoved ipcrr = new IPCRemoteRemoved(recycleBin, trashBin, channelPool);
    if (!ipcrr.apply()) {
      delayedEvents.add(ipcrr);
    }
  }

  /**
   * @return if the owner channel is a client channel.
   * */
  public final boolean isClientChannel() {
    return ownerChannel.getParent() == null;
  }

  /**
   * @return Is close requested. Only for accepted channels.
   * */
  final boolean isCloseRequested() {
    return closeRequested;
  }

  /**
   * @param channelPool the channel pool in which the channel resides.
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * */
  final void reachUpperbound(final ChannelGroup trashBin, final ChannelPrioritySet channelPool) {
    // no delay, may not apply
    applyDelayedTransitionEvents();

    if (channelPool != null) {
      channelPool.getUpdateLock().lock();
    }

    try {
      if (ownerChannel.getParent() != null) {
        // accepted channel
        synchronized (stateMachineLock) {
          if (connected
              && registered
              && inPool
              && !inRecycleBin
              && !inTrashBin
              && !newConnection
              && !closeRequested) {
            inPool = false;
            inTrashBin = true;
            if (channelPool != null) {
              channelPool.remove(ownerChannel);
            }
            trashBin.add(ownerChannel);
          } else {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "reach upperbound Fail: " + ChannelContext.channelToString(ownerChannel));
            }
          }
        }
      } else {
        // client channel
        synchronized (stateMachineLock) {
          if (connected && registered && inPool && !inRecycleBin && !inTrashBin && !newConnection) {
            inPool = false;
            inTrashBin = true;
            if (channelPool != null) {
              channelPool.remove(ownerChannel);
            }
            trashBin.add(ownerChannel);
          } else {
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "reach upperbound Fail: " + ChannelContext.channelToString(ownerChannel));
            }
          }
        }
      }
    } finally {
      if (channelPool != null) {
        channelPool.getUpdateLock().unlock();
      }
    }
  }

  /**
   * Callback if the channel is ready to get closed.
   * */
  final void readyToClose() {
    // no delay, must apply
    applyDelayedTransitionEvents();
    if (ownerChannel.getParent() != null) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected
            && registered
            && !inPool
            && !inRecycleBin
            && inTrashBin
            && !newConnection
            && closeRequested); // {
        connected = false;
        registered = false;
        inTrashBin = false;
        ownerChannel.disconnect();
      }
    } else {
      // client channel
      synchronized (stateMachineLock) {
        if (connected && registered && !inPool && !inRecycleBin && inTrashBin && !newConnection) {
          // if here is to make sure that the message gets sent only once.
          final ChannelFuture cf = ownerChannel.write(IPCMessage.Meta.DISCONNECT);
          disconnectSent();
          cf.addListener(
              new ChannelFutureListener() {

                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                  final Channel ch = future.getChannel();
                  if (!future.isSuccess()) {
                    LOGGER.warn(
                        "Error write disconnect message to channel "
                            + ch
                            + ", cause is "
                            + future.getCause()
                            + ", connected: "
                            + ch.isConnected()
                            + ", disconnect anyway");
                    ch.disconnect();
                  }
                }
              });
        } else {
          if (!IPCUtils.isRemoteConnected(ownerChannel)) {
            // if the remote is already disconnected
            ownerChannel.disconnect();
          }
        }
      }
    }
  }

  /**
   * Record the most recent write future.
   *
   * @param e the most recent message write event.
   * */
  final void recordWriteFuture(final MessageEvent e) {
    mostRecentWriteFuture = e.getFuture();
  }

  /**
   * @param channelPool the channel pool in which the channel resides.
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * */
  final void recycleTimeout(
      final ConcurrentHashMap<Channel, Channel> recycleBin,
      final ChannelGroup trashBin,
      final ChannelPrioritySet channelPool) {
    // nodelay, must apply
    applyDelayedTransitionEvents();

    if (channelPool != null) {
      channelPool.getUpdateLock().lock();
    }

    try {
      if (ownerChannel.getParent() != null) {
        // accepted channel
        synchronized (stateMachineLock) {
          assert (connected
              && registered
              && inPool
              && inRecycleBin
              && !inTrashBin
              && !newConnection
              && !closeRequested);
          inRecycleBin = false;
          inTrashBin = true;
          inPool = false;
          recycleBin.remove(ownerChannel);
          trashBin.add(ownerChannel);
          if (channelPool != null) {
            channelPool.remove(ownerChannel);
          }
        }
      } else {
        // client channel
        synchronized (stateMachineLock) {
          assert (connected
              && registered
              && inPool
              && inRecycleBin
              && !inTrashBin
              && !newConnection);
          inRecycleBin = false;
          inTrashBin = true;
          inPool = false;
          recycleBin.remove(ownerChannel);
          trashBin.add(ownerChannel);
          if (channelPool != null) {
            channelPool.remove(ownerChannel);
          }
        }
      }
    } finally {
      if (channelPool != null) {
        channelPool.getUpdateLock().unlock();
      }
    }
  }

  /**
   *
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
   *          registered).
   * @param remoteID the remote IPC entity ID.
   * */
  final void registerIPCRemoteRemoved(
      final Integer remoteID,
      final ChannelGroup trashBin,
      final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
    // undelayed, must apply
    applyDelayedTransitionEvents();
    registeredContext = new RegisteredChannelContext(remoteID, null);
    if (ownerChannel.getParent() != null) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected
            && !registered
            && !inPool
            && !inRecycleBin
            && !inTrashBin
            && newConnection
            && !closeRequested);
        newConnection = false;
        registered = true;
        inTrashBin = true;
        unregisteredNewChannels.remove(ownerChannel);
        trashBin.add(ownerChannel);
        synchronized (stateMachineLock) {
          for (final EqualityCloseFuture<Integer> ecf : registerConditionFutures) {
            ecf.setActual(remoteID);
          }
        }
      }
    } else {
      // client channel
      synchronized (stateMachineLock) {
        assert (connected
            && !registered
            && !inPool
            && !inRecycleBin
            && !inTrashBin
            && newConnection);
        newConnection = false;
        registered = true;
        unregisteredNewChannels.remove(ownerChannel);
        inTrashBin = true;
        trashBin.add(ownerChannel);
        remoteReply.setFailure(new ChannelException(new IllegalStateException("Remote removed")));
        synchronized (stateMachineLock) {
          for (final EqualityCloseFuture<Integer> ecf : registerConditionFutures) {
            ecf.setActual(remoteID);
          }
        }
      }
    }
  }

  /**
   * @param channelPool the channel pool in which the channel resides.
   * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
   *          registered).
   * @param remoteID the remote IPC entity ID.
   * */
  final void registerNormal(
      final Integer remoteID,
      final ChannelPrioritySet channelPool,
      final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
    // undelayed, must apply
    applyDelayedTransitionEvents();
    registeredContext = new RegisteredChannelContext(remoteID, channelPool);
    channelPool.getUpdateLock().lock();

    try {
      if (ownerChannel.getParent() != null) {
        // accepted channel
        synchronized (stateMachineLock) {
          assert (connected
              && !registered
              && !inPool
              && !inRecycleBin
              && !inTrashBin
              && newConnection
              && !closeRequested);
          newConnection = false;
          registered = true;
          inPool = true;
          unregisteredNewChannels.remove(ownerChannel);
          channelPool.add(ownerChannel);
          synchronized (stateMachineLock) {
            for (final EqualityCloseFuture<Integer> ecf : registerConditionFutures) {
              ecf.setActual(remoteID);
            }
          }
        }
      } else {
        // client channel
        synchronized (stateMachineLock) {
          assert (connected
              && !registered
              && !inPool
              && !inRecycleBin
              && !inTrashBin
              && newConnection);
          newConnection = false;
          registered = true;
          inPool = true;
          registeredContext.incReference();
          channelPool.add(ownerChannel);
          unregisteredNewChannels.remove(ownerChannel);
          synchronized (stateMachineLock) {
            for (final EqualityCloseFuture<Integer> ecf : registerConditionFutures) {
              ecf.setActual(remoteID);
            }
          }
        }
      }
    } finally {
      channelPool.getUpdateLock().unlock();
    }
  }

  /**
   * At connection creation, the client needs to wait the server side sending its IPC ID. This is to make sure that both
   * the client side and the server side are ready to transmit data.
   *
   * @return the remote reply ID.
   * */
  final Integer remoteReplyID() {
    return remoteReplyID;
  }

  /**
   *
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * */
  final void reusedInRecycleTimeout(final ConcurrentHashMap<Channel, Channel> recycleBin) {
    // nodelay, must apply
    applyDelayedTransitionEvents();
    if (ownerChannel.getParent() != null) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected
            && registered
            && inPool
            && inRecycleBin
            && !inTrashBin
            && !newConnection
            && !closeRequested);
        inRecycleBin = false;
        recycleBin.remove(ownerChannel);
      }
    } else {
      // client channel
      synchronized (stateMachineLock) {
        assert (connected && registered && inPool && inRecycleBin && !inTrashBin && !newConnection);
        inRecycleBin = false;
        recycleBin.remove(ownerChannel);
      }
    }
  }

  /**
   * Callback if the owner channel is a client channel and the remote server side has disconnected.
   * */
  final void serverSideDisconnect() {
    assert ownerChannel.getParent() == null;
    applyDelayedTransitionEvents();
    final ServerSideDisconnect ssd = new ServerSideDisconnect();
    if (!ssd.apply()) {
      delayedEvents.add(ssd);
    }
  }

  /**
   * @param remoteID the remote IPC entity ID.
   * */
  final void setRemoteReplyID(final int remoteID) {
    remoteReplyID = remoteID;
    remoteReply.setSuccess();
  }

  /**
   * Update moste recent IO operation on the owner Channel.
   * */
  final void updateLastIOTimestamp() {
    if (inPool) {
      lastIOTimestamp = System.currentTimeMillis();
      registeredContext.updateLastIOTimestamp();
    }
  }

  /**
   * Wait for sometime for the remote to send back it's IPC entity.
   *
   * @return true if remote replied in time.
   * */
  private boolean waitForRemoteReply() {
    if (!remoteReply.isDone()) {
      try {
        remoteReply.await();
      } catch (final InterruptedException e) {
        remoteReply.setFailure(e);
        Thread.currentThread().interrupt();
      } catch (Throwable ee) {
        remoteReply.setFailure(ee);
      }
    }
    return remoteReply.isSuccess();
  }

  /**
   * Do remote channel register
   *
   * For channels initiated by this IPC entity, the registration process is that this IPC entity creates a connection,
   * send my IPC ID, and wait for the remote IPC entity sending back its IPC ID within a timeout.
   *
   * @param myIDMsg the msg encoding my IPC ID to send to remote
   * @param remoteID the remote IPC ID
   * @param registeredChannels registered channels
   * @param unregisteredNewChannels unregistered channels
   * @throws ChannelException if the remote channel registration fails
   */
  final void awaitRemoteRegister(
      final CONNECT myIDMsg,
      final int remoteID,
      final ChannelPrioritySet registeredChannels,
      final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels)
      throws ChannelException {
    remoteReply.addPreListener(
        new OperationFutureListener() {

          @Override
          public void operationComplete(final OperationFuture future) throws Exception {
            if (remoteReplyID == null || remoteID != remoteReplyID) {
              idCheckingTimeout(unregisteredNewChannels);
            } else {
              registerNormal(remoteID, registeredChannels, unregisteredNewChannels);
            }
          }
        });
    ownerChannel.write(myIDMsg);

    if (!waitForRemoteReply()) {
      throw new ChannelException(
          "ID checking timeout, failed to get the remote ID", remoteReply.getCause());
    }

    if (remoteReplyID == null || remoteID != remoteReplyID) {
      throw new ChannelException("ID checking timeout, remote ID doesn't match");
    }
  }

  /**
   * Get a more informative channel string.
   *
   * @param channel the channel
   * @return string
   * */
  public static String channelToString(final Channel channel) {
    if (channel == null) {
      return null;
    }
    ChannelContext cc = getChannelContext(channel);
    StringBuilder sb = new StringBuilder();
    long myID = Long.MAX_VALUE;
    long remoteID = Long.MAX_VALUE;
    if (cc != null) {
      myID = cc.myID;
      RegisteredChannelContext rcc = cc.getRegisteredChannelContext();
      if (rcc != null) {
        remoteID = rcc.getRemoteID();
      }
    }

    sb.append("[");
    if (channel.getParent() == null) {
      // client
      sb.append("Client,");
      if (myID != Long.MAX_VALUE) {
        sb.append("#");
        sb.append((int) myID);
      } else {
        sb.append("#?");
      }
      sb.append(" => ");
      if (remoteID != Long.MAX_VALUE) {
        sb.append("#");
        sb.append((int) remoteID);
      } else {
        sb.append("#?");
      }
    } else {
      // accepted
      sb.append("Accepted,");
      if (remoteID != Long.MAX_VALUE) {
        sb.append("#");
        sb.append((int) remoteID);
      } else {
        sb.append("#?");
      }
      sb.append(" => ");
      if (myID != Long.MAX_VALUE) {
        sb.append("#");
        sb.append((int) myID);
      } else {
        sb.append("#?");
      }
    }
    sb.append("] ");
    sb.append(channel.toString());
    return sb.toString();
  }

  /**
   * Sequentialize channel readability setting.
   */
  private final Object readabilityLock = new Object();

  /**
   * Record the last {@link Channel#setReadable(boolean)} future.
   */
  private ChannelFuture lastReadabilityFuture = null;

  /**
   * Pause read from the ch Channel, this will back-pressure to TCP layer. The TCP stream control will automatically
   * pause the sending from the remote.
   *
   * @param ch the Channel.
   * @return the future instance of the pausing read action
   */
  public static ChannelFuture pauseRead(final Channel ch) {
    ChannelContext cc = ChannelContext.getChannelContext(ch);
    if (cc == null) {
      return ch.setReadable(false);
    }
    final ChannelFuture pauseFuture = Channels.future(ch);
    synchronized (cc.readabilityLock) {
      if (cc.lastReadabilityFuture != null) {
        cc.lastReadabilityFuture.addListener(
            new ChannelFutureListener() {
              @Override
              public void operationComplete(final ChannelFuture future) throws Exception {
                Channels.setInterestOps(
                    ch.getPipeline().getContext(ch.getPipeline().getLast()),
                    pauseFuture,
                    ch.getInterestOps() & ~Channel.OP_READ);
              }
            });
      } else {
        Channels.setInterestOps(
            ch.getPipeline().getContext(ch.getPipeline().getLast()),
            pauseFuture,
            ch.getInterestOps() & ~Channel.OP_READ);
      }
      cc.lastReadabilityFuture = pauseFuture;
    }
    return pauseFuture;
  }

  /**
   * Resume read.
   *
   * @param ch the Channel
   * @return the future instance of the resuming read action
   */
  public static ChannelFuture resumeRead(final Channel ch) {
    ChannelContext cc = ChannelContext.getChannelContext(ch);
    if (cc == null) {
      return ch.setReadable(true);
    }
    final ChannelFuture resumeFuture = Channels.future(ch);
    synchronized (cc.readabilityLock) {
      if (cc.lastReadabilityFuture != null) {
        cc.lastReadabilityFuture.addListener(
            new ChannelFutureListener() {
              @Override
              public void operationComplete(final ChannelFuture future) throws Exception {
                Channels.setInterestOps(
                    ch.getPipeline().getContext(ch.getPipeline().getLast()),
                    resumeFuture,
                    ch.getInterestOps() | Channel.OP_READ);
              }
            });
      } else {
        Channels.setInterestOps(
            ch.getPipeline().getContext(ch.getPipeline().getLast()),
            resumeFuture,
            ch.getInterestOps() | Channel.OP_READ);
      }
      cc.lastReadabilityFuture = resumeFuture;
    }
    return resumeFuture;
  }
}
