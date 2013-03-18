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
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.util.IPCUtils;

/**
 * Recording the various context information of a channel. The most important part of this class is the state machine of
 * a channel.
 * */
public class ChannelContext {

  public class ChannelCloseRequested implements DelayedTransitionEvent {
    private final ChannelPrioritySet channelPool;
    private final ConcurrentHashMap<Channel, Channel> recycleBin;
    private final ChannelGroup trashBin;

    public ChannelCloseRequested(final ChannelPrioritySet channelPool,
        final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin) {
      this.channelPool = channelPool;
      this.recycleBin = recycleBin;
      this.trashBin = trashBin;
    }

    @Override
    public boolean apply() {
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

  public class IDCheckingTimeout implements DelayedTransitionEvent {
    private final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels;

    public IDCheckingTimeout(final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
      this.unregisteredNewChannels = unregisteredNewChannels;
    }

    @Override
    public boolean apply() {
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

  public final class IPCRemoteRemoved implements DelayedTransitionEvent {
    private final ConcurrentHashMap<Channel, Channel> recycleBin;
    private final ChannelGroup trashBin;
    private final ChannelPrioritySet channelPool;

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

    public RegisteredChannelContext(final int remoteID, final ChannelPrioritySet ownerChannelGroup) {
      this.remoteID = remoteID;
      channelGroup = ownerChannelGroup;
      numberOfReference = new AtomicInteger(0);
    }

    public final int decReference() {
      final int newRef = numberOfReference.decrementAndGet();
      if (newRef < 0) {
        final String msg = "Number of references is negative";
        LOGGER.warn(msg);
        throw new IllegalStateException(msg);
      }
      return newRef;
    }

    public final int getRemoteID() {
      return remoteID;
    }

    public final int incReference() {
      return numberOfReference.incrementAndGet();
    }

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

  public class ServerSideDisconnect implements DelayedTransitionEvent {

    @Override
    public boolean apply() {
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
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ChannelContext.class.getName());

  public static final ChannelContext getChannelContext(final Channel channel) {
    return (ChannelContext) channel.getAttachment();
  }

  /**
   * The owner channel of this ChannelContext, i.e. ownerChannel.getAttachment() == this.
   * */
  protected final Channel ownerChannel;

  private volatile boolean alive;

  private volatile ChannelFuture mostRecentWriteFuture = null;

  private final HashSet<EqualityCloseFuture<Integer>> registerConditionFutures;

  private volatile Integer remoteReplyID = null;

  private final Object remoteReplyLock = new Object();

  private final Object channelRegisterLock = new Object();

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

  public Object getAttachment() {
    return attachment.get();
  }

  /**
   * Set attachment to the new value and return old value.
   * 
   * @return the old value.
   * */
  public Object setAttachment(final Object attachment) {
    return this.attachment.getAndSet(attachment);
  }

  public boolean setAttachmentIfAbsent(final Object attachment) {
    return this.attachment.compareAndSet(null, attachment);
  }

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

  public final void closeRequested(final ChannelPrioritySet channelPool,
      final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin) {
    assert !isClientChannel;
    applyDelayedTransitionEvents();
    final ChannelCloseRequested ccr = new ChannelCloseRequested(channelPool, recycleBin, trashBin);
    if (!ccr.apply()) {
      delayedEvents.add(ccr);
    }
  }

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

  public final Channel getChannel() {
    return ownerChannel;
  }

  public final long getLastIOTimestamp() {
    return lastIOTimestamp;
  }

  public final ChannelFuture getMostRecentWriteFuture() {
    return mostRecentWriteFuture;
  }

  public final RegisteredChannelContext getRegisteredChannelContext() {
    return registeredContext;
  }

  public final void idCheckingTimeout(final ConcurrentHashMap<Channel, Channel> unregisteredNewChannels) {
    applyDelayedTransitionEvents();
    final IDCheckingTimeout idct = new IDCheckingTimeout(unregisteredNewChannels);
    if (!idct.apply()) {
      delayedEvents.add(idct);
    }
  }

  public final void ipcRemoteRemoved(final ConcurrentHashMap<Channel, Channel> recycleBin, final ChannelGroup trashBin,
      final ChannelPrioritySet channelPool) {
    applyDelayedTransitionEvents();
    final IPCRemoteRemoved ipcrr = new IPCRemoteRemoved(recycleBin, trashBin, channelPool);
    if (!ipcrr.apply()) {
      delayedEvents.add(ipcrr);
    }
  }

  public final boolean isClientChannel() {
    return isClientChannel;
  }

  /**
   * @return Is close requested. Only for accepted channels.
   * */
  public final boolean isCloseRequested() {
    return closeRequested;
  }

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
          if (!IPCUtils.isRemoteConnected(ownerChannel)) {// if the remote is already disconnected
            ownerChannel.disconnect();
          }
        }
      }
    }
  }

  public final void recordWriteFuture(final MessageEvent e) {
    mostRecentWriteFuture = e.getFuture();
  }

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

  public final Integer remoteReplyID() {
    return remoteReplyID;
  }

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

  public final void serverSideDisconnect() {
    assert isClientChannel;
    applyDelayedTransitionEvents();
    final ServerSideDisconnect ssd = new ServerSideDisconnect();
    if (!ssd.apply()) {
      delayedEvents.add(ssd);
    }
  }

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
