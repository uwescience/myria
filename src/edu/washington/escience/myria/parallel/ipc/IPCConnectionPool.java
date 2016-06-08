package edu.washington.escience.myria.parallel.ipc;

import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelSink;
import org.jboss.netty.channel.DefaultChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.parallel.SocketInfo;
import edu.washington.escience.myria.parallel.ipc.ChannelContext.RegisteredChannelContext;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.concurrent.ErrorLoggingTimerTask;
import edu.washington.escience.myria.util.concurrent.OrderedExecutorService;
import edu.washington.escience.myria.util.concurrent.RenamingThreadFactory;
import edu.washington.escience.myria.util.concurrent.ThreadStackDump;

/**
 * IPCConnectionPool is the hub of inter-process communication. It is consisted of an IPC server (typically a server
 * socket) and a pool of connections.
 *
 * The unit of IPC communication is an IPC entity. The IPC entity who own this connection pool is called the local IPC
 * entity. All the rests are called the remote IPC entities. All IPC entities are indexed by non-negative integers. A
 * negative IPC id means myself.
 *
 * All IPC connections must be created through this class.
 *
 * Usage of IPCConnectionPool:
 *
 * 1. new an IPCConnectionPool class <br/>
 * 2. call start() to actually start run the pool <br/>
 * 3. A single message can be sent through sendShortMessage.<br>
 * 4. A stream of messages of which the order should be kept during IPC transmission should be sent by first
 * reserverALongtermConnection, and then send the stream of messages, and finally releaseLongTermConnection;<br>
 * 5. To shutdown the pool, call shutdown() to shutdown asynchronously or call shutdown().awaitUninterruptedly() to
 * shutdown synchronously.
 */
public final class IPCConnectionPool implements ExternalResourceReleasable {

  /**
   * Self reference IPC ID.
   * */
  public static final int SELF_IPC_ID = Integer.MIN_VALUE;

  /**
   * Channel disconnecter, in charge of checking if the channels in the trash bin are qualified for closing.
   * */
  private class ChannelDisconnecter extends ErrorLoggingTimerTask {
    @Override
    public final synchronized void runInner() {
      final Iterator<Channel> channels = channelTrashBin.iterator();
      while (channels.hasNext()) {
        final Channel c = channels.next();

        final ChannelContext cc = ChannelContext.getChannelContext(c);
        final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
        if (ecc != null) {
          // Disconnecter only considers registered connections
          if (ecc.numReferenced() <= 0) {
            // only close the connection if no one is using the connection.
            // And the connections are closed by the server side.
            if (c.getParent() != null && !c.isReadable()) {
              ChannelContext.resumeRead(c);
            }
            if (c.getParent() == null || (cc.isCloseRequested())) {
              final ChannelFuture cf = cc.getMostRecentWriteFuture();
              if (cf != null) {
                cf.addListener(
                    new ChannelFutureListener() {
                      @Override
                      public void operationComplete(final ChannelFuture future) throws Exception {
                        if (LOGGER.isDebugEnabled()) {
                          LOGGER.debug(
                              "Ready to close a connection: "
                                  + ChannelContext.channelToString(future.getChannel()));
                        }
                        cc.readyToClose();
                      }
                    });
              } else {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("Ready to close a connection: " + ChannelContext.channelToString(c));
                }
                cc.readyToClose();
              }
            }
          }
        }
      }
    }
  }

  /**
   * payload serializer/deserializer.
   * */
  private final PayloadSerializer payloadSerializer;

  /**
   * @return the payload serializer/deserializer
   * */
  public PayloadSerializer getPayloadSerializer() {
    return payloadSerializer;
  }

  /**
   * Recycle unused connections.
   * */
  private class ChannelRecycler extends ErrorLoggingTimerTask {
    @Override
    public final synchronized void runInner() {
      final Iterator<Channel> it = recyclableRegisteredChannels.keySet().iterator();
      while (it.hasNext()) {
        final Channel c = it.next();
        final ChannelContext cc = ChannelContext.getChannelContext(c);
        final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();

        final long recentIOTimestamp = cc.getLastIOTimestamp();
        if (cc.getRegisteredChannelContext().numReferenced() <= 0
            && (System.currentTimeMillis() - recentIOTimestamp)
                >= CONNECTION_RECYCLE_INTERVAL_IN_MS) {
          final ChannelPrioritySet cps = channelPool.get(ecc.getRemoteID()).registeredChannels;
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Recycler decided to close an unused channel: "
                    + ChannelContext.channelToString(c)
                    + ". Remote ID is "
                    + ecc.getRemoteID()
                    + ". Current channelpool size for this remote entity is: "
                    + cps.size());
          }
          cc.recycleTimeout(recyclableRegisteredChannels, channelTrashBin, cps);
        } else {
          cc.reusedInRecycleTimeout(recyclableRegisteredChannels);
        }
      }
    }
  }

  /**
   * recording the info of an remote IPC entity including all the connections between this JVM and the remote IPC
   * entity.
   * */
  private final class IPCRemote {
    /**
     * All the registered connected connections to a remote IPC entity.
     * */
    private final ChannelPrioritySet registeredChannels;

    /**
     * remote IPC entity ID.
     * */
    private final int id;

    /**
     * remote address.
     * */
    private final SocketInfo address;

    /**
     * Connection bootstrap.
     * */
    private final ClientBootstrap bootstrap;

    /**
     * Set of all unregistered channels at the time when this remote entity gets removed.
     * */
    private volatile HashSet<Channel> unregisteredChannelsAtRemove = null;

    /**
     * @param id remote id.
     * @param remoteAddress remote IPC address.
     * @param bootstrap the bootstrap for creating IPC clients to this remote.
     * */
    IPCRemote(final int id, final SocketInfo remoteAddress, final ClientBootstrap bootstrap) {
      this.id = id;
      address = remoteAddress;
      registeredChannels =
          new ChannelPrioritySet(
              POOL_SIZE_LOWERBOUND,
              POOL_SIZE_LOWERBOUND,
              POOL_SIZE_UPPERBOUND,
              new LastIOTimeAscendingComparator());
      this.bootstrap = bootstrap;
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }
  }

  /**
   * A comparator for ranking a set of channels. Unreferenced channels rank before referenced channels, the more
   * referenced, the more backward the channel will be ranked. If two channels have the same referenced number, ranking
   * them according to the most recent IO time stamp.
   * */
  private static class LastIOTimeAscendingComparator implements Comparator<Channel> {

    @Override
    public int compare(final Channel o1, final Channel o2) {
      if (o1 == null || o2 == null) {
        throw new NullPointerException("Channel");
      }

      if (o1 == o2) {
        return 0;
      }

      final ChannelContext o1cc = ChannelContext.getChannelContext(o1);
      final ChannelContext o2cc = ChannelContext.getChannelContext(o2);
      final int o1NR = o1cc.getRegisteredChannelContext().numReferenced();
      final int o2NR = o2cc.getRegisteredChannelContext().numReferenced();
      final long o1IOT = o1cc.getLastIOTimestamp();
      final long o2IOT = o2cc.getLastIOTimestamp();

      if (o1NR != o2NR) {
        return o1NR - o2NR;
      }

      final long diff = o1IOT - o2IOT;
      if (diff > 0) {
        return 1;
      }
      if (diff < 0) {
        return -1;
      }
      return 0;
    }
  }

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(IPCConnectionPool.class);

  /**
   * upper bound of the number of connections between this JVM and a remote IPC entity. Should be moved to the system
   * configuration in the future.
   * */
  public static final int POOL_SIZE_UPPERBOUND = 100;

  /**
   * lower bound of the number of connections between this JVM and a remote IPC entity. Should be moved to the system
   * configuration in the future.
   * */
  public static final int POOL_SIZE_LOWERBOUND = 10;

  /**
   * max number of retry of connecting and writing, etc.
   * */
  public static final int MAX_NUM_RETRY = 3;

  /**
   * connection wait 3 seconds. Should be moved to the system configuration in the future.
   */
  public static final int CONNECTION_WAIT_IN_MS = 3000;

  /**
   * 10 minutes.
   * */
  public static final int CONNECTION_RECYCLE_INTERVAL_IN_MS = 10 * 60 * 1000;

  /**
   * 10 seconds.
   * */
  public static final int CONNECTION_DISCONNECT_INTERVAL_IN_MS = 10000;

  /**
   * Default input buffer capacity for {@link Consumer} input buffers.
   */
  private final int inputBufferCapacity;

  /**
   * @return the system wide default inuput buffer recover event trigger.
   * @see FlowControlBagInputBuffer#INPUT_BUFFER_RECOVER
   */
  private final int inputBufferRecoverTrigger;

  /**
   * pool of connections.
   */
  private final ConcurrentHashMap<Integer, IPCRemote> channelPool;

  /**
   * If the number of connections is between the upper bound and the lower bound. We may drop some connections if the
   * number of independent connections required is not big.
   * */
  private final ConcurrentHashMap<Channel, Channel> recyclableRegisteredChannels;

  /**
   * Groups of connections, which are not needed and to be closed sooner or later, including both registered and
   * unregistered.
   * */
  private final ChannelGroup channelTrashBin;

  /**
   * Pipeline factory which generates pipelines for InJVM channels.
   * */
  private volatile ChannelPipelineFactory localInJVMPipelineFactory;

  /**
   * Channel sink that does the final processing of messages/channel events issued by InJVM channels.
   * */
  private volatile ChannelSink localInJVMChannelSink;

  /**
   * the ID of the IPC entity this connection pool belongs.
   * */
  private final int myID;

  /**
   * IPC server address.
   * */
  private final SocketAddress myIPCServerAddress;

  /**
   * The simple special wrapper channel for transmitting short messages between operators within the same JVM.
   * */
  private volatile InJVMChannel inJVMShortMessageChannel;

  /**
   * IPC server bootstrap.
   * */
  private final ServerBootstrap serverBootstrap;

  /**
   * IPC client bootstrap.
   * */
  private final ClientBootstrap clientBootstrap;

  /**
   * The server channel of this connection pool.
   * */
  private volatile Channel serverChannel;

  /**
   * All Channel instances which can be possibly created through operations of this IPCConnectionPool shall be included
   * in this ChannelGroup.
   * */
  private final DefaultChannelGroup allPossibleChannels;

  /**
   * All channels that are accepted by the IPC server. It is useful when shutting down the IPC server channel.
   * */
  private final DefaultChannelGroup allAcceptedRemoteChannels;

  /**
   * guard the modifications of the set of unregistered channels.
   * */
  private final Object unregisteredChannelSetLock = new Object();

  /**
   * myID TM.
   * */
  private final IPCMessage.Meta.CONNECT myIDMsg;

  /**
   * Denote whether the connection pool has been shutdown.
   * */
  private boolean shutdown = true;

  /**
   * Read write lock to make sure when shutdown, there's no other threads using the IPCConnectionPool.
   * */
  private final ReadWriteLock shutdownLock;

  /**
   * timer for issuing timer tasks.
   * */
  private final ScheduledExecutorService scheduledTaskExecutor;

  /**
   * channel disconnecter.
   * */
  private final ChannelDisconnecter disconnecter;

  /**
   * Recycler.
   * */
  private final ChannelRecycler recycler;

  /**
   * set of unregistered channels.
   * */
  private final ConcurrentHashMap<Channel, Channel> unregisteredChannels;

  /**
   * initial remote addresses.
   * */
  private final Map<Integer, SocketInfo> intialRemoteAddresses;

  /**
   * IPC event processor. All IPC events will be executed by this executor service.
   * */
  private final OrderedExecutorService<Object> ipcEventProcessor;

  /**
   * @return the event processor.
   * */
  OrderedExecutorService<Object> getIPCEventProcessor() {
    return ipcEventProcessor;
  }

  /**
   * @return the input capacity.
   */
  public int getInputBufferCapacity() {
    return inputBufferCapacity;
  }

  /**
   * @return the system wide default inuput buffer recover event trigger.
   * @see FlowControlBagInputBuffer#INPUT_BUFFER_RECOVER
   */
  public int getInputBufferRecoverTrigger() {
    return inputBufferRecoverTrigger;
  }

  /**
   * Construct a connection pool.
   *
   * @param myID self id.
   * @param remoteAddresses remote address mappings.
   * @param serverBootstrap IPC server bootstrap
   * @param clientBootstrap IPC client bootstrap
   * @param payloadSerializer the payload serializer
   * @param mp short message processor
   * @param inputBufferCapacity input buffer capacity
   * @param inputBufferRecoverTrigger input buffer recover trigger.
   * */
  public IPCConnectionPool(
      final int myID,
      final Map<Integer, SocketInfo> remoteAddresses,
      final ServerBootstrap serverBootstrap,
      final ClientBootstrap clientBootstrap,
      final PayloadSerializer payloadSerializer,
      final ShortMessageProcessor<?> mp,
      final int inputBufferCapacity,
      final int inputBufferRecoverTrigger) {
    this.myID = myID;
    this.inputBufferCapacity = inputBufferCapacity;
    this.inputBufferRecoverTrigger = inputBufferRecoverTrigger;
    myIDMsg = new IPCMessage.Meta.CONNECT(myID);
    myIPCServerAddress = remoteAddresses.get(myID).getBindAddress();
    this.clientBootstrap = clientBootstrap;
    this.serverBootstrap = serverBootstrap;

    channelPool = new ConcurrentHashMap<Integer, IPCRemote>();

    intialRemoteAddresses = remoteAddresses;

    recyclableRegisteredChannels = new ConcurrentHashMap<Channel, Channel>();
    unregisteredChannels = new ConcurrentHashMap<Channel, Channel>();

    channelTrashBin = new DefaultChannelGroup();

    scheduledTaskExecutor =
        Executors.newSingleThreadScheduledExecutor(
            new RenamingThreadFactory("IPC connection pool global timer"));
    disconnecter = new ChannelDisconnecter();
    recycler = new ChannelRecycler();
    allPossibleChannels = new DefaultChannelGroup();
    allAcceptedRemoteChannels = new DefaultChannelGroup();
    shutdownFuture = new ConditionChannelGroupFuture();
    consumerChannelMap = new ConcurrentHashMap<StreamIOChannelID, StreamInputBuffer<?>>();
    ipcEventProcessor =
        new OrderedExecutorService<Object>(
            1,
            Runtime.getRuntime().availableProcessors(),
            new RenamingThreadFactory("IPC connection pool event processor"));
    this.payloadSerializer = payloadSerializer;
    shortMessageProcessor = mp;
    shutdownLock = new ReentrantReadWriteLock();
  }

  /**
   * @return my IPC ID.
   * */
  public int getMyIPCID() {
    return myID;
  }

  /**
   * Check if the IPC pool is already shutdown.
   *
   * @throws IllegalStateException if the pool is already shutdown
   * */
  private void checkShutdown() throws IllegalStateException {
    if (shutdown) {
      final String msg = "IPC connection pool already shutdown.";
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn(msg);
      }
      throw new IllegalStateException(msg);
    }
  }

  /**
   * Check if the remote IPC entity is in the pool.
   *
   * @param remoteID remote ID.
   * @return true if remote is still alive, false otherwise.
   * */
  public boolean isRemoteValid(final int remoteID) {
    return channelPool.containsKey(remoteID);
  }

  /**
   * Detect if the remote IPC entity is still alive or not.
   *
   * TODO more robust dead checking
   *
   * @param remoteID remote ID.
   * @return true if remote is still alive, false otherwise.
   * */
  public boolean isRemoteAlive(final int remoteID) {
    Channel ch = null;
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        // already shutdown
        return false;
      }

      if (!channelPool.containsKey(remoteID)) {
        // remoteID is not valid
        return false;
      }

      try {
        ch = getAConnection(remoteID);

        if (!IPCUtils.isRemoteConnected(ch)) {
          // shallow testing
          return false;
        }

        boolean ss = ch.write(IPCMessage.Meta.PING).awaitUninterruptibly().isSuccess();
        if (!ss) {
          return false;
        }
        return true;
      } catch (ChannelException ee) {
        return false;
      } catch (RuntimeException ee) {
        throw ee;
      } catch (Exception ee) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Error in testing remote alive, treat it as dead.", ee);
        }
      } finally {
        if (ch != null) {
          ChannelContext.getChannelContext(ch).getRegisteredChannelContext().decReference();
        }
      }
      return false;
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * A remote IPC entity has requested to close the channel.
   *
   * @param channel the channel to be closed.
   * @return channel close future.
   * */
  @Nonnull
  ChannelFuture closeChannelRequested(final Channel channel) {
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        return channel.close();
      }
      final ChannelContext cc = ChannelContext.getChannelContext(channel);
      final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
      final IPCRemote remote = channelPool.get(ecc.getRemoteID());

      if (remote != null) {
        cc.closeRequested(remote.registeredChannels, recyclableRegisteredChannels, channelTrashBin);
      } else {
        cc.closeRequested(null, recyclableRegisteredChannels, channelTrashBin);
      }
      return channel.getCloseFuture();
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * close a channel.
   *
   * @param ch the channel
   * @return close future
   * */
  @Nonnull
  private ChannelFuture closeUnregisteredChannel(final Channel ch) {
    final ChannelContext context = ChannelContext.getChannelContext(ch);
    if (context != null) {
      final ChannelFuture writeFuture = context.getMostRecentWriteFuture();
      if (writeFuture != null) {
        writeFuture.awaitUninterruptibly();
      }
    }
    if (ch.isOpen()) {
      ch.close()
          .addListener(
              new ChannelFutureListener() {
                @Override
                public void operationComplete(final ChannelFuture future) throws Exception {
                  final ChannelPipeline cp = future.getChannel().getPipeline();
                  if (cp instanceof ExternalResourceReleasable) {
                    ((ExternalResourceReleasable) cp).releaseExternalResources();
                  }
                }
              });
    }
    return ch.getCloseFuture();
  }

  /**
   * Connect to remoteAddress with timeout connectionTimeoutMS.
   *
   * @return the nio channel if succeed, otherwise an Exception will be thrown.
   * @param remote the remote info.
   * @param connectionTimeoutMS timeout.
   * @param ic connector;
   * @throws ChannelException if any error occurs in creating new connections in the Netty layer
   * */
  @Nonnull
  private Channel createANewConnection(
      final IPCRemote remote, final long connectionTimeoutMS, final ClientBootstrap ic)
      throws ChannelException {
    ChannelFuture c = null;
    c = ic.connect(remote.address.getConnectAddress());
    if (connectionTimeoutMS > 0) {
      c.awaitUninterruptibly(connectionTimeoutMS);
    } else {
      c.awaitUninterruptibly();
    }
    if (c.isSuccess()) {
      final Channel channel = c.getChannel();
      if (channel.isConnected()) {
        final ChannelContext cc = new ChannelContext(channel, myID);
        channel.setAttachment(cc);
        cc.connected();
        cc.awaitRemoteRegister(myIDMsg, remote.id, remote.registeredChannels, unregisteredChannels);
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "Created a new registered channel from: {} to {}. Channel: {}",
              myID,
              remote.id,
              ChannelContext.channelToString(channel),
              new ThreadStackDump());
        }
        allPossibleChannels.add(channel);
        return channel;
      }
    }
    closeUnregisteredChannel(c.getChannel());
    throw new ChannelException(c.getCause());
  }

  /**
   * @return get a connection to a remote IPC entity with ID id. The connection may be newly created or an existing one
   *         from the connection pool.
   * @param ipcIDP the remote ID.
   * @throws IllegalStateException if the pool is already shutdown
   * @throws ChannelException if any error occurs in the Netty layer
   * */
  @Nonnull
  private Channel getAConnection(final int ipcIDP) throws IllegalStateException, ChannelException {
    shutdownLock.readLock().lock();
    try {
      checkShutdown();
      int ipcID = ipcIDP;
      if (ipcIDP == SELF_IPC_ID) {
        ipcID = myID;
      }
      final IPCRemote remote = channelPool.get(ipcID);
      if (ipcID == myID) {
        try {
          InJVMChannel ch =
              new InJVMChannel(localInJVMPipelineFactory.getPipeline(), localInJVMChannelSink);
          final ChannelContext cc = new ChannelContext(ch, myID);
          ch.setAttachment(cc);
          cc.connected();
          cc.registerNormal(myID, remote.registeredChannels, unregisteredChannels);
          return ch;
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          // error in pipeline creation
          throw new ChannelException(e);
        }
      }

      if (remote == null || remote.unregisteredChannelsAtRemove != null) {
        // id is invalid || remote already get removed
        throw new IllegalStateException("Remote doesn't exist");
      }

      Channel channel = null;
      int retry = 0;
      ChannelException failure = null;
      while ((retry < MAX_NUM_RETRY) && (channel == null)) {
        if (retry > 1 && LOGGER.isDebugEnabled()) {
          LOGGER.debug("Retry creating a connection to id#" + ipcIDP, new ThreadStackDump());
        }
        failure = null;
        try {
          // always create new connections if needed.
          channel = remote.registeredChannels.peekAndReserve();
          if (channel == null) {
            channel = createANewConnection(remote, CONNECTION_WAIT_IN_MS, remote.bootstrap);
          } else {
            final ChannelContext cc = ChannelContext.getChannelContext(channel);
            final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
            if (ecc.numReferenced() > 1) {
              /*
               * otherwise if createANewConnetion throws an exception, channel is still not null outside of this while
               * loop.
               */
              channel = null;
              ecc.decReference();
              channel = createANewConnection(remote, CONNECTION_WAIT_IN_MS, remote.bootstrap);
            }
          }
        } catch (ChannelException e) {
          if (failure == null) {
            failure = e;
          }
        } catch (Exception e) {
          if (failure == null) {
            failure = new ChannelException(e);
          }
        }
        retry++;
      }

      if (failure != null) {
        throw failure;
      }
      if (channel == null) {
        // fail to connect
        throw new ChannelException("Fail to connect to " + ipcIDP + "");
      }

      ChannelContext.getChannelContext(channel).updateLastIOTimestamp();
      return channel;
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * @return my id as TM.
   * */
  @Nonnull
  IPCMessage.Meta.CONNECT getMyIDAsMsg() {
    return myIDMsg;
  }

  /**
   * @return if the IPC pool is shutdown already.
   * */
  public boolean isShutdown() {
    shutdownLock.readLock().lock();
    try {
      return shutdown;
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * the IPC server has accepted a new channel.
   *
   * @param newChannel new accepted channel.
   * */
  void newAcceptedRemoteChannel(final Channel newChannel) {
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        // stop accepting new connections if the connection pool is already shutdown.
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Already shutdown, new remote channel directly close. Channel: "
                  + ChannelContext.channelToString(newChannel));
        }
        newChannel.close();
        return;
      }
      allPossibleChannels.add(newChannel);
      allAcceptedRemoteChannels.add(newChannel);
      newChannel.setAttachment(new ChannelContext(newChannel, myID));
      synchronized (unregisteredChannelSetLock) {
        unregisteredChannels.put(newChannel, newChannel);
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * Add or modify remoteID -> remoteAddress mappings.
   *
   * @param remoteID remoteID to put.
   * @param remoteAddress remote address.
   * */
  public void putRemote(final int remoteID, final SocketInfo remoteAddress) {
    shutdownLock.readLock().lock();
    try {
      checkShutdown();
      if (remoteID == myID || remoteID == SELF_IPC_ID) {
        return;
      }
      final IPCRemote newOne = new IPCRemote(remoteID, remoteAddress, clientBootstrap);
      final IPCRemote oldOne = channelPool.put(remoteID, newOne);
      if (oldOne == null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("new IPC remote entity added: " + newOne, new ThreadStackDump());
        }
      } else {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              "Existing IPC remote entity changed from " + oldOne + " to " + newOne,
              new ThreadStackDump());
        }
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * Other workers/the master initiate connection to this worker/master. Add the connection to the pool.
   *
   * @param remoteIDP remoteID.
   * @param channel the new channel.
   * */
  void registerChannel(final int remoteIDP, final Channel channel) {
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        channel.close();
        return;
      }
      int remoteID = remoteIDP;
      if (remoteIDP == SELF_IPC_ID) {
        remoteID = myID;
      }
      IPCRemote remote = channelPool.get(remoteID);
      if (remote == null) {
        final String msg =
            "Unknown remote, id: " + remoteID + " address: " + channel.getRemoteAddress();
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(msg);
        }
        throw new IllegalStateException(msg);
      }

      if (channel.getParent() != serverChannel) {
        final String msg =
            "Channel "
                + ChannelContext.channelToString(channel)
                + " does not belong to the connection pool";
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(msg);
        }
        throw new IllegalArgumentException(msg);
      }

      final ChannelContext cc = ChannelContext.getChannelContext(channel);
      if (remote.unregisteredChannelsAtRemove != null) {
        // already removed
        if (remote.unregisteredChannelsAtRemove.contains(channel)) {
          cc.registerIPCRemoteRemoved(remoteID, channelTrashBin, unregisteredChannels);
        } else {
          final String msg =
              "Unknown remote, id: " + remoteID + " address: " + channel.getRemoteAddress();
          if (LOGGER.isWarnEnabled()) {
            LOGGER.warn(msg);
          }
          throw new IllegalStateException(msg);
        }
      } else {
        cc.registerNormal(remoteID, remote.registeredChannels, unregisteredChannels);
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * @param channel the channel.
   * @return channel release future.
   * */
  @Nonnull
  public ChannelFuture releaseLongTermConnection(final StreamOutputChannel<?> channel) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Released long-term connection " + channel, new ThreadStackDump());
    }
    shutdownLock.readLock().lock();
    try {
      Channel ch = channel.getIOChannel();
      if (ch != null) {
        if (!shutdown) {
          return this.releaseLongTermConnection(ch);
        } else {
          return ch.close();
        }
      } else {
        ChannelFuture cf = new DefaultChannelFuture(NullChannel.NULL, false);
        cf.setSuccess();
        return cf;
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * @param channel the channel.
   * @return channel release future.
   * */
  @Nonnull
  private ChannelFuture releaseLongTermConnection(final Channel channel) {
    Preconditions.checkNotNull(channel);

    final ChannelContext cc = ChannelContext.getChannelContext(channel);
    final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
    ChannelFuture cf = null;

    if (ecc == null) {
      cf = new DefaultChannelFuture(channel, false);
      cf.setSuccess();
      channelTrashBin.add(channel);
    } else {
      cf = channel.write(IPCMessage.Meta.EOS);
      cf.addListener(
          new ChannelFutureListener() {

            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {

              ChannelContext cc = ChannelContext.getChannelContext(future.getChannel());
              cc.getRegisteredChannelContext().getIOPair().deMapOutputChannel();

              final Channel channel = future.getChannel();
              if (channel instanceof InJVMChannel) {
                channel.close();
                return;
              }

              final IPCRemote r = channelPool.get(ecc.getRemoteID());
              if (r != null) {
                r.registeredChannels.release(
                    channel, channelTrashBin, recyclableRegisteredChannels);
              } else {
                ecc.decReference();
              }
            }
          });
    }
    return cf;
  }

  /**
   *
   * @param remoteID remoteID to remove.
   * @return a Future object. If the remoteID is in the connection pool, and with a non-empty set of established
   *         connections, the method will try close these connections asynchronously. Future object is for looking up
   *         the progress of closing. Otherwise an empty done future object.
   * */
  @Nonnull
  public ChannelGroupFuture removeRemote(final int remoteID) {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "remove the remote entity #{} from IPC connection pool", remoteID, new ThreadStackDump());
    }
    shutdownLock.readLock().lock();
    try {
      if (remoteID == myID || remoteID == SELF_IPC_ID || shutdown) {
        return new DefaultChannelGroupFuture(
            new DefaultChannelGroup(), Collections.<ChannelFuture>emptySet());
      }
      final IPCRemote old = channelPool.get(remoteID);
      if (old != null) {
        Channel[] uChannels = new Channel[] {};
        synchronized (unregisteredChannelSetLock) {
          if (!unregisteredChannels.isEmpty()) {
            uChannels = unregisteredChannels.keySet().toArray(uChannels);
          }
        }
        final DefaultChannelGroup connectionsToDisconnect = new DefaultChannelGroup();
        final Collection<ChannelFuture> futures = new LinkedList<ChannelFuture>();

        if (uChannels.length != 0) {
          old.unregisteredChannelsAtRemove = new HashSet<Channel>(Arrays.asList(uChannels));
        } else {
          channelPool.remove(remoteID);
        }

        Channel[] channels = new Channel[] {};
        channels = old.registeredChannels.toArray(channels);

        for (final Channel ch : channels) {
          ChannelContext.getChannelContext(ch)
              .ipcRemoteRemoved(
                  recyclableRegisteredChannels, channelTrashBin, old.registeredChannels);
          connectionsToDisconnect.add(ch);
          futures.add(ch.getCloseFuture());
        }

        for (final Channel ch : uChannels) {
          final EqualityCloseFuture<Integer> registerFuture =
              new EqualityCloseFuture<Integer>(ch, remoteID);
          ChannelContext.getChannelContext(ch).addConditionFuture(registerFuture);
          futures.add(registerFuture);
        }

        final DefaultChannelGroupFuture cgf =
            new DefaultChannelGroupFuture(connectionsToDisconnect, futures);

        cgf.addListener(
            new ChannelGroupFutureListener() {
              @Override
              public void operationComplete(final ChannelGroupFuture future) throws Exception {
                channelPool.remove(remoteID, old);
              }
            });
        return cgf;
      }
      return new DefaultChannelGroupFuture(
          new DefaultChannelGroup(), Collections.<ChannelFuture>emptySet());
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * @return export shutdown lock for use in {@link StreamOutputChannel}.
   * */
  @Nonnull
  ReadWriteLock getShutdownLock() {
    return shutdownLock;
  }

  /**
   *
   * get an IO channel.
   *
   * @param id of a remote IPC entity
   * @param streamID of a stream
   * @return IPC channel, null if id is invalid or connect fails.
   * @throws IllegalStateException if the connection pool is already shutdown
   * @param <PAYLOAD> the IPC message payload type
   */
  @CheckForNull
  public <PAYLOAD> StreamOutputChannel<PAYLOAD> reserveLongTermConnection(
      final int id, final long streamID) {
    shutdownLock.readLock().lock();
    try {
      checkShutdown();
      Channel ch = getAConnection(id);
      // write bos even a recovery channel otherwise EOS from a non-stream
      ch.write(new IPCMessage.Meta.BOS(streamID));
      ChannelContext cc = ((ChannelContext) (ch.getAttachment()));
      int remoteID = cc.getRegisteredChannelContext().getRemoteID();
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(
            "reserve long term connection for ({},{}), attached to physical connection {}",
            id,
            streamID,
            ChannelContext.channelToString(ch),
            new ThreadStackDump());
      }
      return new StreamOutputChannel<PAYLOAD>(new StreamIOChannelID(streamID, remoteID), this, ch);
    } catch (ChannelException e) {
      LOGGER.warn("Unable to connect to remote. Cause is: ", e);
    } finally {
      shutdownLock.readLock().unlock();
    }
    return null;
  }

  /**
   * Send a message to a remote IPC entity without reserving a connection.
   *
   * @return write future, non-null.
   * @param ipcID IPC ID.
   * @param message the message to send.
   * @throws IllegalStateException if the connection pool is already shutdown
   * @param <PAYLOAD> the payload type
   * */
  @Nonnull
  public <PAYLOAD> ChannelFuture sendShortMessage(final int ipcID, final PAYLOAD message)
      throws IllegalStateException {
    shutdownLock.readLock().lock();
    try {
      checkShutdown();
      if (ipcID == myID || ipcID == SELF_IPC_ID) {
        return inJVMShortMessageChannel.write(message);
      }
      Channel ch;
      try {
        ch = getAConnection(ipcID);
      } catch (ChannelException e) {
        DefaultChannelFuture r = new DefaultChannelFuture(NullChannel.NULL, false);
        r.setFailure(e);
        return r;
      }
      final ChannelFuture cf = ch.write(message);
      cf.addListener(
          new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
              final Channel ch = future.getChannel();
              if (!(ch instanceof InJVMChannel) && !shutdown) {
                final ChannelContext cc = ChannelContext.getChannelContext(ch);
                final ChannelContext.RegisteredChannelContext ecc =
                    cc.getRegisteredChannelContext();
                ecc.decReference();
              }
            }
          });
      return cf;
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * @param inputBuffer register the inputbuffer. Setup the input channel IDs -> input buffer mapping.
   * @throws IllegalStateException if any of the inputBuffer's inputChannels have been linked to an existing input
   *           buffer
   * */
  public void registerStreamInput(final StreamInputBuffer<?> inputBuffer)
      throws IllegalStateException {
    Preconditions.checkNotNull(inputBuffer);
    shutdownLock.readLock().lock();
    try {
      checkShutdown();
      ImmutableSet<StreamIOChannelID> sourceChannels = inputBuffer.getSourceChannels();
      for (StreamIOChannelID id : sourceChannels) {
        if (id.getRemoteID() == SELF_IPC_ID) {
          id = new StreamIOChannelID(id.getStreamID(), myID);
        }
        StreamInputBuffer<?> ic = consumerChannelMap.putIfAbsent(id, inputBuffer);
        if (ic != null) {
          throw new IllegalArgumentException(
              "Input channel: "
                  + id
                  + " is already linked to an input buffer, with procesor: "
                  + ic.getProcessor());
        }
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * @param inputBuffer de-register the inputbuffer. Clean up the input channel IDs -> input buffer mapping.
   * */
  public void deRegisterStreamInput(final StreamInputBuffer<?> inputBuffer) {
    Preconditions.checkNotNull(inputBuffer);
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        return;
      }
      ImmutableSet<StreamIOChannelID> sourceChannels = inputBuffer.getSourceChannels();
      for (StreamIOChannelID id : sourceChannels) {
        if (id.getRemoteID() == SELF_IPC_ID) {
          id = new StreamIOChannelID(id.getStreamID(), myID);
        }
        consumerChannelMap.remove(id, inputBuffer);
        StreamInputChannel<?> sic = inputBuffer.getInputChannel(id);
        Channel c = sic.getIOChannel();
        if (c != null) {
          ChannelContext cc = ChannelContext.getChannelContext(c);
          cc.getRegisteredChannelContext().getIOPair().deMapInputChannel();
        }
      }
      inputBuffer.stop();
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * @return short message processor.
   * @param <PAYLOAD> the payload type.
   * */
  @SuppressWarnings("unchecked")
  <PAYLOAD> ShortMessageProcessor<PAYLOAD> getShortMessageProcessor() {
    return (ShortMessageProcessor<PAYLOAD>) shortMessageProcessor;
  }

  /**
   * @param inputID the input channel ID.
   * @return the input buffer for the input channel ID.
   * @param <PAYLOAD> the payload type of the input buffer.
   * */
  @SuppressWarnings("unchecked")
  @CheckForNull
  <PAYLOAD> StreamInputBuffer<PAYLOAD> getInputBuffer(final StreamIOChannelID inputID) {
    StreamIOChannelID ecID = inputID;
    if (inputID.getRemoteID() == SELF_IPC_ID) {
      ecID = new StreamIOChannelID(inputID.getStreamID(), myID);
    }
    return (StreamInputBuffer<PAYLOAD>) consumerChannelMap.get(ecID);
  }

  /**
   * input buffer mapping.
   * */
  private final ConcurrentHashMap<StreamIOChannelID, StreamInputBuffer<?>> consumerChannelMap;

  /**
   * Close all the connections abruptly. Do not call this method to shutdown IPC pool if not necessary. Call
   * {@link #shutdown()} instead.
   * <p>
   *
   * This method may cause {@link ClosedChannelException} to the channels currently in use by operators if the operators
   * try to read/write data from/to the channels. And also it may cause data loss if the data is buffered but has not
   * yet feed to the operators.
   * <p>
   *
   * Remote IPC entities won't expect this IPC to be shutdown in this way. So it may also cause
   * {@link ClosedChannelException} to the channels currently in use by operators at remote sites.
   *
   * @return the future instance, which will be called back if all the connections have been closed or any error occurs.
   * */
  @Nonnull
  public ChannelGroupFuture shutdownNow() {
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("Abrupt shutdown of IPC connection pool is requested!");
    }
    shutdownLock.writeLock().lock();
    try {
      if (shutdown) {
        // already shutdown
        return shutdownFuture;
      }
      shutdown = true;
      inJVMShortMessageChannel.close();
      // shutdown timer tasks, take over all the controls.
      scheduledTaskExecutor.shutdownNow();
      ipcEventProcessor.shutdownNow();
      synchronized (disconnecter) {
        synchronized (recycler) {
          // make sure all the timer tasks are done.
          channelPool.clear();
          channelTrashBin.clear();
          synchronized (unregisteredChannelSetLock) {
            unregisteredChannels.clear();
          }
          allAcceptedRemoteChannels.clear();
          recyclableRegisteredChannels.clear();
          intialRemoteAddresses.clear();

          final ChannelGroupFuture closeAll = allPossibleChannels.close();

          shutdownFuture.setBackedChannelGroupFuture(closeAll);
          shutdownFuture.setCondition(true);
          return shutdownFuture;
        }
      }
    } finally {
      shutdownLock.writeLock().unlock();
    }
  }

  /**
   * Future for pool shut down.
   * */
  private final ConditionChannelGroupFuture shutdownFuture;

  /**
   * Short message processor.
   * */
  private final ShortMessageProcessor<?> shortMessageProcessor;

  /**
   * This method will always return the same Future instance for an IPCConnectionPool.
   *
   * @return the future instance for the shutdown event of this pool.
   *
   */
  public ChannelGroupFuture getShutdownFuture() {
    return shutdownFuture;
  }

  /**
   * Shutdown this IPC connection pool. All the connections will be released.
   *
   * Semantic of shutdown: <br/>
   * 1. No connections can be got <br/>
   * 2. No messages can be sent <br/>
   * 3. Connections already registered can be accepted, because there may be already some data in the input buffer of
   * the registered connections<br/>
   * 4. As long as read/write buffers are empty, close the connections
   *
   * @return the future instance, which will be called back if all the connections have been closed or any error occurs.
   * */
  @Nonnull
  public ChannelGroupFuture shutdown() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("IPC connection pool is going to shutdown");
    }
    shutdownLock.writeLock().lock();
    try {
      if (shutdown) {
        // already shutdown
        return shutdownFuture;
      }
      shutdown = true;
      final Iterator<Channel> acceptedChIt = allAcceptedRemoteChannels.iterator();
      final Collection<ChannelFuture> allAcceptedChannelCloseFutures =
          new LinkedList<ChannelFuture>();
      while (acceptedChIt.hasNext()) {
        final Channel ch = acceptedChIt.next();
        allAcceptedChannelCloseFutures.add(ch.getCloseFuture());
      }
      new DefaultChannelGroupFuture(allAcceptedRemoteChannels, allAcceptedChannelCloseFutures)
          .addListener(
              new ChannelGroupFutureListener() {

                @Override
                public void operationComplete(final ChannelGroupFuture future) throws Exception {
                  serverChannel
                      .unbind(); // shutdown server channel only if all the accepted connections have been
                  // disconnected.
                }
              });

      inJVMShortMessageChannel.close();

      final Collection<ChannelFuture> allConnectionMostRecentWriteFutures =
          new ArrayList<ChannelFuture>(allPossibleChannels.size());

      scheduledTaskExecutor.shutdown();
      ipcEventProcessor.shutdown();
      synchronized (disconnecter) {
        synchronized (recycler) {
          // make sure all the timer tasks are done.
          final Integer[] remoteIDs = channelPool.keySet().toArray(new Integer[] {});

          for (final Integer remoteID : remoteIDs) {
            removeRemote(remoteID);
          }
          for (final Channel ch : allPossibleChannels) {
            ChannelContext cc = ChannelContext.getChannelContext(ch);
            if (cc != null) {
              RegisteredChannelContext rcc = cc.getRegisteredChannelContext();
              if (rcc != null) {
                rcc.clearReference();
              }
              allConnectionMostRecentWriteFutures.add(cc.getMostRecentWriteFuture());
            }
          }
          final DefaultChannelGroupFuture writeAll =
              new DefaultChannelGroupFuture(
                  allPossibleChannels, allConnectionMostRecentWriteFutures);
          writeAll.addListener(
              new ChannelGroupFutureListener() {
                @Override
                public void operationComplete(final ChannelGroupFuture future) throws Exception {
                  shutdownFuture.setBackedChannelGroupFuture(allPossibleChannels.close());
                  shutdownFuture.setCondition(true);
                }
              });
          return shutdownFuture;
        }
      }
    } finally {
      shutdownLock.writeLock().unlock();
    }
  }

  /**
   * Callback if error encountered for a channel.
   *
   * @param ch the error channel
   * */
  void channelDisconnected(final Channel ch) {
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        return;
      }
      ChannelContext cc = ChannelContext.getChannelContext(ch);
      if (cc != null) {
        RegisteredChannelContext rcc = cc.getRegisteredChannelContext();
        if (rcc != null) {
          IPCRemote remote = channelPool.get(rcc.getRemoteID());
          if (remote != null) {
            cc.closed(
                unregisteredChannels,
                recyclableRegisteredChannels,
                channelTrashBin,
                remote.registeredChannels);
          }
        } else {
          cc.closed(unregisteredChannels, recyclableRegisteredChannels, channelTrashBin, null);
        }
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * Callback if error encountered for a channel.
   *
   * @param ch the error channel
   * @param cause the cause of the error.
   * */
  void errorEncountered(final Channel ch, final Throwable cause) {
    shutdownLock.readLock().lock();
    try {
      if (shutdown) {
        ch.close();
        return;
      }
      ChannelContext cc = ChannelContext.getChannelContext(ch);
      if (cc == null) {
        ch.close();
      } else {
        RegisteredChannelContext rcc = cc.getRegisteredChannelContext();
        if (rcc != null) {
          IPCRemote remote = channelPool.get(rcc.getRemoteID());
          if (remote != null) {
            cc.errorEncountered(
                unregisteredChannels,
                recyclableRegisteredChannels,
                channelTrashBin,
                remote.registeredChannels,
                cause);
          }
        } else {
          cc.errorEncountered(
              unregisteredChannels, recyclableRegisteredChannels, channelTrashBin, null, cause);
        }
      }
    } finally {
      shutdownLock.readLock().unlock();
    }
  }

  /**
   * Start the pool service. all the external resources are allocated at this point.
   *
   * @param serverChannelFactory IPC server channel factory
   * @param serverPipelineFactory IPC server pipeline factory
   * @param clientChannelFactory IPC client channel factory
   * @param clientPipelineFactory IPC client pipeline factory
   * @param localInJVMPipelineFactory IPC in JVM channel pipeline factory
   * @param localInJVMChannelSink IPC in JVM channel sink.
   *
   * @throws Exception if any error occurs.
   * */
  public void start(
      final ChannelFactory serverChannelFactory,
      final ChannelPipelineFactory serverPipelineFactory,
      final ChannelFactory clientChannelFactory,
      final ChannelPipelineFactory clientPipelineFactory,
      final ChannelPipelineFactory localInJVMPipelineFactory,
      final ChannelSink localInJVMChannelSink)
      throws Exception {
    shutdownLock.writeLock().lock();
    try {
      if (!shutdown) {
        throw new IllegalStateException("Connection pool already started.");
      }
      shutdown = false;
      serverBootstrap.setFactory(serverChannelFactory);
      serverBootstrap.setPipelineFactory(serverPipelineFactory);
      serverChannel = serverBootstrap.bind(myIPCServerAddress);

      clientBootstrap.setFactory(clientChannelFactory);
      clientBootstrap.setPipelineFactory(clientPipelineFactory);

      for (final Integer id : intialRemoteAddresses.keySet()) {
        channelPool.put(id, new IPCRemote(id, intialRemoteAddresses.get(id), clientBootstrap));
      }
      IPCRemote myself = channelPool.get(myID);
      if (myself == null) {
        myself = new IPCRemote(myID, intialRemoteAddresses.get(myID), clientBootstrap);
        IPCRemote old = channelPool.putIfAbsent(myID, myself);
        if (old != null) {
          myself = old;
        }
      }

      this.localInJVMChannelSink = localInJVMChannelSink;
      this.localInJVMPipelineFactory = localInJVMPipelineFactory;

      inJVMShortMessageChannel =
          new InJVMChannel(localInJVMPipelineFactory.getPipeline(), localInJVMChannelSink);
      final ChannelContext cc = new ChannelContext(inJVMShortMessageChannel, myID);
      inJVMShortMessageChannel.setAttachment(cc);
      cc.connected();
      cc.registerNormal(myID, myself.registeredChannels, unregisteredChannels);

      allPossibleChannels.add(serverChannel);
      scheduledTaskExecutor.scheduleAtFixedRate(
          recycler,
          (int) ((1 + Math.random()) * CONNECTION_RECYCLE_INTERVAL_IN_MS / 2),
          CONNECTION_RECYCLE_INTERVAL_IN_MS / 2,
          TimeUnit.MILLISECONDS);
      scheduledTaskExecutor.scheduleAtFixedRate(
          disconnecter,
          (int) ((1 + Math.random()) * CONNECTION_DISCONNECT_INTERVAL_IN_MS / 2),
          CONNECTION_DISCONNECT_INTERVAL_IN_MS / 2,
          TimeUnit.MILLISECONDS);
    } finally {
      shutdownLock.writeLock().unlock();
    }
  }

  /**
   * release the external resources this connection pool is using. Especially, the thread pools used in
   * ChannelFactories.
   * */
  @Override
  public void releaseExternalResources() {
    shutdown()
        .addListener(
            new ChannelGroupFutureListener() {
              @Override
              public void operationComplete(final ChannelGroupFuture future) throws Exception {
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("pre release resources");
                }
                Thread t =
                    new Thread("IPC resource releaser") {
                      @Override
                      public void run() {
                        serverBootstrap.shutdown();
                        clientBootstrap.shutdown();
                        serverBootstrap.releaseExternalResources();
                        clientBootstrap.releaseExternalResources();
                      }
                    };
                t.start();
                if (LOGGER.isDebugEnabled()) {
                  LOGGER.debug("post release resources");
                }
              }
            });
  }
}
