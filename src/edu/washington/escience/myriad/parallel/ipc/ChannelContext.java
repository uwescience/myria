package edu.washington.escience.myriad.parallel.ipc;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.util.IPCUtils;

/**
 * Recording the various context information of a channel. The most important part of this class is the state machine of
 * a channel.
 * */
public class ChannelContext {

  /**
   * Channel close requested event.
   * */
  public class ChannelCloseRequested implements DelayedTransitionEvent {
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
    public ChannelCloseRequested(final ChannelPrioritySet channelPool,
        final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin) {
      this.channelPool = channelPool;
      this.recycleBin = recycleBin;
      this.trashBin = trashBin;
    }

    @Override
    public final boolean apply() {
      // accepted channel
      synchronized (stateMachineLock) {
        if (connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection && !closeRequested
            || connected && registered && inPool && !inRecycleBin && !inTrashBin && !newConnection && !closeRequested
            || connected && registered && inPool && inRecycleBin && !inTrashBin && !newConnection && !closeRequested
            || connected && registered && !inPool && !inRecycleBin && inTrashBin && !newConnection && !closeRequested) {
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
  public interface DelayedTransitionEvent {
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
  public class IDCheckingTimeout implements DelayedTransitionEvent {
    /**
     * Set of new channels who have not identified there worker IDs yet (i.e. not registered).
     * */
    private final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels;

    /**
     * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
     *          registered).
     * */
    public IDCheckingTimeout(final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
      this.unregisteredNewChannels = unregisteredNewChannels;
    }

    @Override
    public final boolean apply() {
      if (!isClientChannel) {
        // accepted channel
        synchronized (stateMachineLock) {
          if ((!connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection && !closeRequested)
              || (connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection && !closeRequested)) {
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
          if ((!connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection)
              || (connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection)) {
            newConnection = false;
            if (connected) {
              ownerChannel.disconnect();
            }
            connected = false;
            unregisteredNewChannels.remove(ownerChannel);

            synchronized (channelRegisterLock) {
              channelRegisterLock.notifyAll();
            }

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
  public final class IPCRemoteRemoved implements DelayedTransitionEvent {
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
    public IPCRemoteRemoved(final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin,
        final ChannelPrioritySet channelPool) {
      this.recycleBin = recycleBin;
      this.trashBin = trashBin;
      this.channelPool = channelPool;
    }

    @Override
    public boolean apply() {
      if (!isClientChannel) {
        // accepted channel
        synchronized (stateMachineLock) {
          if ((connected && registered && inPool && inRecycleBin && !inTrashBin && !newConnection && !closeRequested)
              || (connected && registered && inPool && !inRecycleBin && !inTrashBin && !newConnection && !closeRequested)) {
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
              || (connected && registered && inPool && !inRecycleBin && !inTrashBin && !newConnection)) {
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
      return true;
    }
  }

  /**
   * Extra state data of a registered channel.
   * */
  public class RegisteredChannelContext {

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
    private final AtomicInteger numberOfReference;

    /**
     * @param remoteID the remote IPC entity ID.
     * @param ownerChannelGroup which channel set the channel belongs.
     * */
    public RegisteredChannelContext(final int remoteID, final ChannelPrioritySet ownerChannelGroup) {
      this.remoteID = remoteID;
      channelGroup = ownerChannelGroup;
      numberOfReference = new AtomicInteger(0);
    }

    /**
     * Decrease the number of references by 1.
     * 
     * @return the new number of references.
     * */
    public final int decReference() {
      final int newRef = numberOfReference.decrementAndGet();
      if (newRef < 0) {
        final String msg = "Number of references is negative";
        LOGGER.warn(msg);
        throw new IllegalStateException(msg);
      }
      return newRef;
    }

    /**
     * @return the remote IPC entity ID.
     * */
    public final int getRemoteID() {
      return remoteID;
    }

    /**
     * Increase the number of references by 1.
     * 
     * @return the new number of references.
     * */
    public final int incReference() {
      return numberOfReference.incrementAndGet();
    }

    /**
     * @return current number of references.
     * */
    public final int numReferenced() {
      return numberOfReference.get();
    }

    /**
     * @return If the owner channel group get reordered
     * */
    public final boolean updateLastIOTimestamp() {
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
  public class ServerSideDisconnect implements DelayedTransitionEvent {

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
   * If the owner channel is still alive.
   * */
  private volatile boolean alive;

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
   * The lock for remote ID reply condition.
   * */
  private final Object remoteReplyLock = new Object();

  /**
   * The lock for channel registration condition.
   * */
  private final Object channelRegisterLock = new Object();

  /**
   * An attachment for customized extra functionality.
   * */
  private final AtomicReference<Object> attachment;

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
  private boolean inPool = false;
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
   * record if the owner channel is a client channel.
   * */
  private final boolean isClientChannel;

  /**
   * @param channel the owner channel
   * @param isClientChannel if the owner channel is a client channel.
   * */
  public ChannelContext(final Channel channel, final boolean isClientChannel) {
    this.isClientChannel = isClientChannel;
    lastIOTimestamp = System.currentTimeMillis();
    ownerChannel = channel;
    alive = true;
    closeRequested = false;
    stateMachineLock = new Object();
    registeredContext = null;
    registerConditionFutures = new HashSet<EqualityCloseFuture<Integer>>();
    delayedEvents = new ConcurrentLinkedQueue<DelayedTransitionEvent>();
    attachment = new AtomicReference<Object>();
  }

  /**
   * @return attachment.
   * */
  public final Object getAttachment() {
    return attachment.get();
  }

  /**
   * Set attachment to the new value and return old value.
   * 
   * @return the old value.
   * @param attachment the new attachment
   * */
  public final Object setAttachment(final Object attachment) {
    return this.attachment.getAndSet(attachment);
  }

  /**
   * Set attachment to the value only if currently no attachment is set.
   * 
   * @return if the attachment is already set, return false, otherwise true.
   * @param attachment the attachment to set
   * */
  public final boolean setAttachmentIfAbsent(final Object attachment) {
    return this.attachment.compareAndSet(null, attachment);
  }

  /**
   * @param future the future for channel registration
   * */
  public final void addConditionFuture(final EqualityCloseFuture<Integer> future) {
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
  public final void closeRequested(final ChannelPrioritySet channelPool,
      final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin) {
    assert !isClientChannel;
    applyDelayedTransitionEvents();
    final ChannelCloseRequested ccr = new ChannelCloseRequested(channelPool, recycleBin, trashBin);
    if (!ccr.apply()) {
      delayedEvents.add(ccr);
    }
  }

  /**
   * Callback when the owner channel is connected.
   * */
  public final void connected() {
    applyDelayedTransitionEvents();
    // undelayed, must apply
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert !connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection && !closeRequested;
        connected = true;
      }
    } else {
      // client channel
      synchronized (stateMachineLock) {
        assert !connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection;
        connected = true;
      }
    }
  }

  /**
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * 
   * */
  public final void considerRecycle(final ConcurrentHashMap<Channel, Channel> recycleBin) {
    // undelayed, may not apply
    applyDelayedTransitionEvents();
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        if (connected && registered && inPool && !inRecycleBin && !inTrashBin && !newConnection && !closeRequested) {
          inRecycleBin = true;
          recycleBin.put(ownerChannel, ownerChannel);
        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("consider recycle unsatisfied: " + ownerChannel);
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
          LOGGER.debug("consider recycle unsatisfied: " + ownerChannel);
        }
      }
    }
  }

  /**
   * Callback when the owner channel has sent disconnect request to the remote part.
   * */
  public final void disconnectSent() {
    // undelayed, must apply
    assert isClientChannel;
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
  public final Channel getChannel() {
    return ownerChannel;
  }

  /**
   * @return the timestamp of last IO operation.
   * */
  public final long getLastIOTimestamp() {
    return lastIOTimestamp;
  }

  /**
   * @return the write future of the most recent write action.
   * */
  public final ChannelFuture getMostRecentWriteFuture() {
    return mostRecentWriteFuture;
  }

  /**
   * @return the extra channel context data structure if the channel is registered.
   * */
  public final RegisteredChannelContext getRegisteredChannelContext() {
    return registeredContext;
  }

  /**
   * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
   *          registered).
   * */
  public final void idCheckingTimeout(final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
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
  public final void ipcRemoteRemoved(final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin,
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
    return isClientChannel;
  }

  /**
   * @return Is close requested. Only for accepted channels.
   * */
  public final boolean isCloseRequested() {
    return closeRequested;
  }

  /**
   * @param channelPool the channel pool in which the channel resides.
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * */
  public final void reachUpperbound(final ChannelGroup trashBin, final ChannelPrioritySet channelPool) {
    // no delay, may not apply
    applyDelayedTransitionEvents();
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        if (connected && registered && inPool && !inRecycleBin && !inTrashBin && !newConnection && !closeRequested) {
          inPool = false;
          inTrashBin = true;
          if (channelPool != null) {
            channelPool.remove(ownerChannel);
          }
          trashBin.add(ownerChannel);
        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("reach upperbound Fail: " + ownerChannel);
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
            LOGGER.debug("reach upperbound Fail: " + ownerChannel);
          }
        }
      }
    }
  }

  /**
   * Callback if the channel is ready to get closed.
   * */
  public final void readyToClose() {
    // no delay, must apply
    applyDelayedTransitionEvents();
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected && registered && !inPool && !inRecycleBin && inTrashBin && !newConnection && closeRequested); // {
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
          final ChannelFuture cf = ownerChannel.write(IPCUtils.CONTROL_DISCONNECT);
          disconnectSent();
          cf.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
              final Channel ch = future.getChannel();
              if (!future.isSuccess()) {
                LOGGER.warn("Error write disconnect message to channel " + ch + ", cause is " + future.getCause()
                    + ", connected: " + ch.isConnected() + ", disconnect anyway");
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
  public final void recordWriteFuture(final MessageEvent e) {
    mostRecentWriteFuture = e.getFuture();
  }

  /**
   * @param channelPool the channel pool in which the channel resides.
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * */
  public final void recycleTimeout(final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin,
      final ChannelPrioritySet channelPool) {
    // nodelay, must apply
    applyDelayedTransitionEvents();
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected && registered && inPool && inRecycleBin && !inTrashBin && !newConnection && !closeRequested);
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
        assert (connected && registered && inPool && inRecycleBin && !inTrashBin && !newConnection);
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
  }

  /**
   * 
   * @param trashBin channel trash bin. The place where to-be-closed channels reside.
   * @param unregisteredNewChannels Set of new channels who have not identified there worker IDs yet (i.e. not
   *          registered).
   * @param remoteID the remote IPC entity ID.
   * */
  public final void registerIPCRemoteRemoved(final Integer remoteID, final ChannelGroup trashBin,
      final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
    // undelayed, must apply
    applyDelayedTransitionEvents();
    registeredContext = new RegisteredChannelContext(remoteID, null);
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection && !closeRequested);
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
        assert (connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection);
        newConnection = false;
        registered = true;
        unregisteredNewChannels.remove(ownerChannel);
        inTrashBin = true;
        trashBin.add(ownerChannel);
        synchronized (channelRegisterLock) {
          channelRegisterLock.notifyAll();
        }
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
  public final void registerNormal(final Integer remoteID, final ChannelPrioritySet channelPool,
      final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
    // undelayed, must apply
    applyDelayedTransitionEvents();
    registeredContext = new RegisteredChannelContext(remoteID, channelPool);
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection && !closeRequested);
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
        assert (connected && !registered && !inPool && !inRecycleBin && !inTrashBin && newConnection);
        newConnection = false;
        registered = true;
        inPool = true;
        registeredContext.incReference();
        channelPool.add(ownerChannel);
        unregisteredNewChannels.remove(ownerChannel);
        synchronized (channelRegisterLock) {
          channelRegisterLock.notifyAll();
        }
        synchronized (stateMachineLock) {
          for (final EqualityCloseFuture<Integer> ecf : registerConditionFutures) {
            ecf.setActual(remoteID);
          }
        }
      }
    }
  }

  /**
   * At connection creation, the client needs to wait the server side sending its IPC ID. This is to make sure that both
   * the client side and the server side are ready to transmit data.
   * 
   * @return the remote reply ID.
   * */
  public final Integer remoteReplyID() {
    return remoteReplyID;
  }

  /**
   * 
   * @param recycleBin channel recycle bin. The place where currently-unused-but-waiting-for-possible-reuse channels
   *          reside.
   * */
  public final void reusedInRecycleTimeout(final ConcurrentHashMap<Channel, Channel> recycleBin) {
    // nodelay, must apply
    applyDelayedTransitionEvents();
    if (!isClientChannel) {
      // accepted channel
      synchronized (stateMachineLock) {
        assert (connected && registered && inPool && inRecycleBin && !inTrashBin && !newConnection && !closeRequested);
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
  public final void serverSideDisconnect() {
    assert isClientChannel;
    applyDelayedTransitionEvents();
    final ServerSideDisconnect ssd = new ServerSideDisconnect();
    if (!ssd.apply()) {
      delayedEvents.add(ssd);
    }
  }

  /**
   * @param remoteID the remote IPC entity ID.
   * */
  public final void setRemoteReplyID(final int remoteID) {
    remoteReplyID = remoteID;
    synchronized (remoteReplyLock) {
      remoteReplyLock.notifyAll();
    }
    synchronized (channelRegisterLock) {
      while (newConnection) {
        try {
          channelRegisterLock.wait();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * Update moste recent IO operation on the owner Channel.
   * */
  public final void updateLastIOTimestamp() {
    if (alive) {
      lastIOTimestamp = System.currentTimeMillis();
      if (registeredContext != null) {
        if (!registeredContext.updateLastIOTimestamp()) {
          alive = false;
        }
      }
    }
  }

  /**
   * Wait for sometime for the remote to send back it's IPC entity.
   * 
   * @param timeoutInMillis the time out
   * @return true if remote replied in time.
   * */
  public final boolean waitForRemoteReply(final long timeoutInMillis) {
    if (remoteReplyID == null) {
      synchronized (remoteReplyLock) {
        try {
          remoteReplyLock.wait(timeoutInMillis);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    return remoteReplyID != null;
  }
}
