package edu.washington.escience.myriad.parallel;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.ChannelGroupFutureListener;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.ExternalResourceReleasable;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;

/**
 * IPCConnectionPool is the hub of inter-process communication. It is consisted of an IPC server (typically a server
 * socket) and a pool of connections.
 * 
 * The unit of IPC communication is an IPC entity. The IPC entity who own this connection pool is called the local IPC
 * entity. All the rests are called the remote IPC entities. All IPC entities are indexed by integers.
 * 
 * Index 0 is reserved for the master. Workers can be any other integers.
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
public class IPCConnectionPool {

  /**
   * Channel disconnecter, in charge of checking if the channels in the trash bin are qualified for closing.
   * */
  protected class ChannelDisconnecter implements Runnable {
    @Override
    public final synchronized void run() {
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
            if (cc.isClientChannel() || (cc.isCloseRequested())) {
              final ChannelFuture cf = cc.getMostRecentWriteFuture();
              if (cf != null) {
                cf.addListener(new ChannelFutureListener() {
                  @Override
                  public void operationComplete(final ChannelFuture future) throws Exception {
                    LOGGER.debug("Ready to close a connection: " + future.getChannel());
                    cc.readyToClose(channelTrashBin);
                  }
                });
              } else {
                LOGGER.debug("Ready to close a connection: " + c);
                cc.readyToClose(channelTrashBin);
              }
            }
          }
        }
      }
    }
  }

  /**
   * Check the registration of new connections.
   * */
  protected class ChannelIDChecker implements Runnable {
    @Override
    public final synchronized void run() {
      synchronized (unregisteredChannelSetLock) {
        final Iterator<Channel> it = unregisteredChannels.keySet().iterator();
        while (it.hasNext()) {
          final Channel c = it.next();
          final ChannelContext cc = ChannelContext.getChannelContext(c);
          if ((System.currentTimeMillis() - cc.getLastIOTimestamp()) >= CONNECTION_ID_CHECK_TIMEOUT_IN_MS) {
            cc.idCheckingTimeout(unregisteredChannels);
          }
        }
      }
    }
  }

  /**
   * Recycle unused connections.
   * */
  protected class ChannelRecycler implements Runnable {
    @Override
    public final synchronized void run() {
      final Iterator<Channel> it = recyclableRegisteredChannels.keySet().iterator();
      while (it.hasNext()) {
        final Channel c = it.next();
        final ChannelContext cc = ChannelContext.getChannelContext(c);
        final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();

        final long recentIOTimestamp = cc.getLastIOTimestamp();
        if (cc.getRegisteredChannelContext().numReferenced() <= 0
            && (System.currentTimeMillis() - recentIOTimestamp) >= CONNECTION_RECYCLE_INTERVAL_IN_MS) {
          final ChannelPrioritySet cps = channelPool.get(ecc.getRemoteID()).registeredChannels;
          LOGGER.debug("Recycler decided to close an unused channel: " + c + ". Remote ID is " + ecc.getRemoteID()
              + ". Current channelpool size for this remote entity is: " + cps.size());
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
    final ChannelPrioritySet registeredChannels;

    /**
     * remote IPC entity ID.
     * */
    final Integer id;

    /**
     * remote address.
     * */
    final SocketInfo address;

    /**
     * Connection bootstrap.
     * */
    final ClientBootstrap bootstrap;

    /**
     * Set of all unregistered channels at the time when this remote entity gets removed.
     * */
    volatile HashSet<Channel> unregisteredChannelsAtRemove = null;

    IPCRemote(final Integer id, final SocketInfo remoteAddress) {
      this.id = id;
      address = remoteAddress;
      registeredChannels =
          new ChannelPrioritySet(POOL_SIZE_LOWERBOUND, POOL_SIZE_LOWERBOUND, POOL_SIZE_UPPERBOUND,
              new LastIOTimeAscendingComparator());
      bootstrap = ParallelUtility.createIPCClient(clientChannelFactory, clientPipelineFactory);
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
  static class LastIOTimeAscendingComparator implements Comparator<Channel> {

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
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IPCConnectionPool.class.getName());

  /**
   * upper bound of the number of connections between this JVM and a remote IPC entity. Should be moved to the system
   * configuration in the future.
   * */
  public static final int POOL_SIZE_UPPERBOUND = 50;

  /**
   * lower bound of the number of connections between this JVM and a remote IPC entity. Should be moved to the system
   * configuration in the future.
   * */
  public static final int POOL_SIZE_LOWERBOUND = 3;

  /**
   * max number of retry of connecting and writing, etc.
   * */
  public static final int MAX_NUM_RETRY = 3;

  /**
   * data transfer timeout 30 seconds.Should be moved to the system configuration in the future.
   */
  public static final int DATA_TRANSFER_TIMEOUT_IN_MS = 30000;

  /**
   * connection wait 10 seconds. Should be moved to the system configuration in the future.
   */
  public static final int CONNECTION_WAIT_IN_MS = 10000;

  /**
   * 10 minutes.
   * */
  public static final int CONNECTION_RECYCLE_INTERVAL_IN_MS = 10 * 60 * 1000;

  /**
   * 10 seconds.
   * */
  public static final int CONNECTION_DISCONNECT_INTERVAL_IN_MS = 10000;

  /**
   * connection id check time out.
   * */
  public static final int CONNECTION_ID_CHECK_TIMEOUT_IN_MS = 10000;

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
   * NIO client side factory.
   * */
  private ChannelFactory clientChannelFactory;

  /**
   * pipeline factory.
   * */
  private final ChannelPipelineFactory clientPipelineFactory;

  /**
   * the ID of the IPC entity this connection pool belongs.
   * */
  private final int myID;

  private final SocketAddress myIPCServerAddress;

  /**
   * The simple special wrapper channel for transmitting data between operators within the same JVM.
   * */
  private final InJVMChannel inJVMChannel;

  /**
   * IPC server bootstrap.
   * */
  private ServerBootstrap serverBootstrap;

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
  private final TransportMessage myIDTM;

  /**
   * Denote whether the connection pool has been shutdown.
   * */
  private volatile boolean shutdown = false;

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
   * id checker.
   * */
  private final ChannelIDChecker idChecker;

  /**
   * set of unregistered channels.
   * */
  private final ConcurrentHashMap<Channel, Channel> unregisteredChannels;

  private final Map<Integer, SocketInfo> remoteAddresses;

  private final LinkedBlockingQueue<MessageWrapper> messageQueue;

  /**
   * Construct a connection pool.
   * 
   * @param myID self id.
   * @param remoteAddresses remote address mappings.
   * @param messageQueue my message queue.
   * */
  public IPCConnectionPool(final int myID, final Map<Integer, SocketInfo> remoteAddresses,
      final LinkedBlockingQueue<MessageWrapper> messageQueue) {

    this.myID = myID;
    this.messageQueue = messageQueue;
    myIDTM = IPCUtils.connectTM(myID);
    inJVMChannel = new InJVMChannel(myID, messageQueue);
    myIPCServerAddress = remoteAddresses.get(myID).getAddress();
    if (myID == 0) {
      clientPipelineFactory = new IPCPipelineFactories.MasterClientPipelineFactory(messageQueue, this);
    } else {
      clientPipelineFactory = new IPCPipelineFactories.WorkerClientPipelineFactory(messageQueue, this);
    }

    channelPool = new ConcurrentHashMap<Integer, IPCRemote>();

    this.remoteAddresses = remoteAddresses;

    recyclableRegisteredChannels = new ConcurrentHashMap<Channel, Channel>();
    unregisteredChannels = new ConcurrentHashMap<Channel, Channel>();

    channelTrashBin = new DefaultChannelGroup();

    // ipcGlobalTimer = new Timer();
    scheduledTaskExecutor = Executors.newSingleThreadScheduledExecutor();
    disconnecter = new ChannelDisconnecter();
    idChecker = new ChannelIDChecker();
    recycler = new ChannelRecycler();
    allPossibleChannels = new DefaultChannelGroup();
    allAcceptedRemoteChannels = new DefaultChannelGroup();
  }

  /**
   * Check if the IPC pool is already shutdown.
   * */
  private void checkShutdown() {
    if (shutdown) {
      final String msg = "IPC connection pool already shutdown.";
      LOGGER.warn(msg);
      throw new IllegalStateException(msg);
    }
  }

  /**
   * A remote IPC entity has requested to close the channel.
   * 
   * @param channel the channel to be closed.
   * @return channel close future.
   * */
  final ChannelFuture closeChannelRequested(final Channel channel) {
    final ChannelContext cc = ChannelContext.getChannelContext(channel);
    final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
    final IPCRemote remote = channelPool.get(ecc.getRemoteID());

    if (remote != null) {
      cc.closeRequested(remote.registeredChannels, recyclableRegisteredChannels, channelTrashBin);
    } else {
      cc.closeRequested(null, recyclableRegisteredChannels, channelTrashBin);
    }
    return channel.getCloseFuture();
  }

  /**
   * close a channel.
   * 
   * @param ch the channel
   * @return close future
   * */
  private ChannelFuture closeUnregisteredChannel(final Channel ch) {
    if (ch != null) {
      final ChannelContext context = ChannelContext.getChannelContext(ch);
      if (context != null) {
        final ChannelFuture writeFuture = context.getMostRecentWriteFuture();
        if (writeFuture != null) {
          writeFuture.awaitUninterruptibly();
        }
      }

      if (ch.isConnected()) {
        ch.disconnect();
      }
      if (ch.isOpen()) {
        final ChannelFuture cf = ch.close();
        cf.addListener(new ChannelFutureListener() {
          @Override
          public void operationComplete(final ChannelFuture future) throws Exception {
            final ChannelPipeline cp = future.getChannel().getPipeline();
            if (cp instanceof ExternalResourceReleasable) {
              ((ExternalResourceReleasable) cp).releaseExternalResources();
            }
          }
        });
        return cf;
      }
    }
    return null;
  }

  /**
   * Connect to remoteAddress with timeout connectionTimeoutMS.
   * 
   * @return the nio channel if succeed, null otherwise.
   * @param remote the remote info.
   * @param connectionTimeoutMS timeout.
   * @param ic connector;
   * */
  private Channel createANewConnection(final IPCRemote remote, final long connectionTimeoutMS, final ClientBootstrap ic) {
    boolean connected = true;
    ChannelFuture c = null;
    try {
      c = ic.connect(remote.address.getAddress());
    } catch (final Exception e) {
      connected = false;
    }
    if (connected) {
      int retry = 0;
      connected = false;
      while (!connected && retry < MAX_NUM_RETRY) {
        if (connectionTimeoutMS > 0) {
          c.awaitUninterruptibly(connectionTimeoutMS);
        } else {
          c.awaitUninterruptibly();
        }
        connected = c.isSuccess();
        retry++;
      }
    }
    if (connected) {
      final Channel channel = c.getChannel();
      final ChannelContext cc = new ChannelContext(channel, true);
      channel.setAttachment(cc);
      cc.connected();
      final ChannelFuture idWriteFuture = channel.write(myIDTM);
      int retry = 0;
      while (retry < MAX_NUM_RETRY && !idWriteFuture.awaitUninterruptibly(DATA_TRANSFER_TIMEOUT_IN_MS)) {
        // tell the remote part my id
        retry++;
      }

      if (retry >= MAX_NUM_RETRY || !idWriteFuture.isSuccess()) {
        cc.idCheckingTimeout(unregisteredChannels);
        return null;
      }

      retry = 0;
      while (retry < MAX_NUM_RETRY && !cc.waitForRemoteReply(CONNECTION_ID_CHECK_TIMEOUT_IN_MS)) {
        retry++;
      }

      if (retry >= MAX_NUM_RETRY || !(remote.id.equals(cc.remoteReplyID()))) {
        cc.idCheckingTimeout(unregisteredChannels);
        return null;
      }

      cc.registerNormal(remote.id, remote.registeredChannels, unregisteredChannels);
      cc.getRegisteredChannelContext().incReference();
      LOGGER.debug("Created a new registered channel from: " + myID + ", to: " + remote.id + ". Channel: " + channel);
      allPossibleChannels.add(channel);
      return channel;
    } else {
      if (c != null) {
        closeUnregisteredChannel(c.getChannel());
      }
      return null;
    }
  }

  /**
   * @return get a connection to a remote IPC entity with ID id. The connection may be newly created or an existing one
   *         from the connection pool.
   * @param remoteId the remote ID.
   * 
   * */
  private Channel getAConnection(final int remoteId) {
    checkShutdown();
    if (remoteId == myID) {
      return inJVMChannel;
    }
    final IPCRemote remote = channelPool.get(remoteId);
    if (remote == null) {
      // id is invalid
      return null;
    }

    if (remote.unregisteredChannelsAtRemove != null) {
      // remote already get removed
      return null;
    }

    Channel channel = null;
    int retry = 0;

    while ((retry < MAX_NUM_RETRY) && (channel == null)) {
      // get a connection instance
      channel = remote.registeredChannels.peekAndReserve();
      if (channel == null) {
        channel = createANewConnection(remote, CONNECTION_WAIT_IN_MS, remote.bootstrap);
      } else if (remote.registeredChannels.size() < POOL_SIZE_UPPERBOUND) {
        final ChannelContext cc = ChannelContext.getChannelContext(channel);
        final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
        if (ecc.numReferenced() > 1) {
          // it's not a free connection, since we have not reached the upper bound, new a
          // connection
          ecc.decReference();
          channel = createANewConnection(remote, CONNECTION_WAIT_IN_MS, remote.bootstrap);
        }
      }
      retry++;
    }

    if (channel == null) {
      // fail to connect
      return null;
    }

    ChannelContext.getChannelContext(channel).updateLastIOTimestamp();

    return channel;
  }

  /**
   * @return my id as TM.
   * */
  final TransportMessage getMyIDAsTM() {
    return myIDTM;
  }

  /**
   * @return if the IPC pool is shutdown already.
   * */
  public final boolean isShutdown() {
    return shutdown;
  }

  /**
   * the IPC server has accepted a new channel.
   * 
   * @param newChannel new accepted channel.
   * */
  final void newAcceptedRemoteChannel(final Channel newChannel) {
    if (shutdown) {
      // stop accepting new connections if the connection pool is already shutdown.
      LOGGER.warn("Already shutdown, new remote channel directly close. Channel: "
          + ToStringBuilder.reflectionToString(newChannel));
      newChannel.close();
    }
    allPossibleChannels.add(newChannel);
    allAcceptedRemoteChannels.add(newChannel);
    newChannel.setAttachment(new ChannelContext(newChannel, false));
    synchronized (unregisteredChannelSetLock) {
      unregisteredChannels.put(newChannel, newChannel);
    }
  }

  /**
   * Add or modify remoteID -> remoteAddress mappings.
   * 
   * @param remoteID remoteID to put.
   * @param remoteAddress remote address.
   * */
  public final void putRemote(final Integer remoteID, final SocketInfo remoteAddress) {
    if (remoteID == null || remoteID == myID) {
      return;
    }
    final IPCRemote newOne = new IPCRemote(remoteID, remoteAddress);
    final IPCRemote oldOne = channelPool.put(remoteID, newOne);
    if (oldOne == null) {
      LOGGER.debug("new IPC remote entity added: " + newOne);
    } else {
      LOGGER.debug("Existing IPC remote entity changed from " + oldOne + " to " + newOne);
    }
  }

  /**
   * Other workers/the master initiate connection to this worker/master. Add the connection to the pool.
   * 
   * @param remoteID remoteID.
   * @param channel the new channel.
   * */
  final void registerChannel(final Integer remoteID, final Channel channel) {
    final IPCRemote remote = channelPool.get(remoteID);
    if (remote == null) {
      final String msg = "Unknown remote, id: " + remoteID + " address: " + channel.getRemoteAddress();
      LOGGER.warn(msg);
      throw new IllegalStateException(msg);
    }

    if (channel.getParent() != serverChannel) {
      final String msg = "Channel " + channel + " does not belong to the connection pool";
      LOGGER.warn(msg);
      throw new IllegalArgumentException(msg);
    }

    final ChannelContext cc = ChannelContext.getChannelContext(channel);
    if (remote.unregisteredChannelsAtRemove != null) {
      // already removed
      if (remote.unregisteredChannelsAtRemove.contains(channel)) {
        cc.registerIPCRemoteRemoved(remoteID, channelTrashBin, unregisteredChannels);
      } else {
        final String msg = "Unknown remote, id: " + remoteID + " address: " + channel.getRemoteAddress();
        LOGGER.warn(msg);
        throw new IllegalStateException(msg);
      }
    } else {
      cc.registerNormal(remoteID, remote.registeredChannels, unregisteredChannels);
    }
  }

  /**
   * @param channel the channel.
   * */
  public final void releaseLongTermConnection(final Channel channel) {
    if (channel == null) {
      return;
    }
    if (channel == inJVMChannel) {
      return;
    }
    final ChannelContext cc = ChannelContext.getChannelContext(channel);
    final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
    if (ecc == null) {
      channelTrashBin.add(channel);
      // closeChannel(channel);
      return;
    }
    final IPCRemote r = channelPool.get(ecc.getRemoteID());
    if (r != null) {
      r.registeredChannels.release(channel, channelTrashBin, recyclableRegisteredChannels);
    }

  }

  /**
   * 
   * @param remoteID remoteID to remove.
   * @return a Future object. If the remoteID is in the connection pool, and with a non-empty set of established
   *         connections, the method will try close these connections asynchronously. Future object is for looking up
   *         the progress of closing. Otherwise, null.
   * */
  public final ChannelGroupFuture removeRemote(final Integer remoteID) {
    LOGGER.debug("remove the remote entity #" + remoteID + " from IPC connection pool");
    if (remoteID == null || remoteID == myID) {
      return null;
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
        ChannelContext.getChannelContext(ch).ipcRemoteRemoved(recyclableRegisteredChannels, channelTrashBin,
            old.registeredChannels);
        connectionsToDisconnect.add(ch);
        futures.add(ch.getCloseFuture());
      }

      for (final Channel ch : uChannels) {
        final EqualityCloseFuture<Integer> registerFuture = new EqualityCloseFuture<Integer>(ch, remoteID);
        ChannelContext.getChannelContext(ch).addConditionFuture(registerFuture);
        futures.add(registerFuture);
      }

      final DefaultChannelGroupFuture cgf = new DefaultChannelGroupFuture(connectionsToDisconnect, futures);

      cgf.addListener(new ChannelGroupFutureListener() {
        @Override
        public void operationComplete(final ChannelGroupFuture future) throws Exception {
          new Thread() {
            @Override
            public void run() {
              channelPool.remove(remoteID);
            }
          }.start();
        }
      });
      return cgf;
    }
    return null;
  }

  /**
   * 
   * get an IO channel.
   * 
   * @param id of a remote IPC entity
   * @return IPC channel, null if id is invalid or connect fails.
   * @throws IllegalStateException if the connection pool is already shutdown
   */
  public final Channel reserveLongTermConnection(final int id) {
    checkShutdown();
    return getAConnection(id);
  }

  /**
   * Send a message to a remote IPC entity without reserving a connection.
   * 
   * @return write future.
   * @param remoteID remote iD.
   * @param message the message to send.
   * @throws IllegalStateException if the connection pool is already shutdown
   * */
  public final ChannelFuture sendShortMessage(final Integer remoteID, final TransportMessage message) {
    checkShutdown();
    final Channel ch = getAConnection(remoteID);
    if (ch == null || !ch.isConnected()) {
      return null;
    }
    final ChannelFuture cf = ch.write(message);
    cf.addListener(new ChannelFutureListener() {

      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {
        final Channel ch = future.getChannel();
        if (ch != inJVMChannel) {
          final ChannelContext cc = ChannelContext.getChannelContext(ch);
          final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
          ecc.decReference();
        }
      }
    });
    return cf;
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
  public final ChannelGroupFuture shutdown() {
    shutdown = true;
    LOGGER.debug("IPC connection pool is going to shutdown");
    final Iterator<Channel> acceptedChIt = allAcceptedRemoteChannels.iterator();
    final Collection<ChannelFuture> allAcceptedChannelCloseFutures = new LinkedList<ChannelFuture>();
    while (acceptedChIt.hasNext()) {
      final Channel ch = acceptedChIt.next();
      allAcceptedChannelCloseFutures.add(ch.getCloseFuture());
    }
    new DefaultChannelGroupFuture(allAcceptedRemoteChannels, allAcceptedChannelCloseFutures)
        .addListener(new ChannelGroupFutureListener() {

          @Override
          public void operationComplete(final ChannelGroupFuture future) throws Exception {
            serverChannel.unbind(); // shutdown server channel only if all the accepted connections have been
                                    // disconnected.
          }
        });

    inJVMChannel.close();

    final ChannelFuture serverCloseFuture = serverChannel.getCloseFuture();

    final Collection<ChannelFuture> allConnectionCloseFutures = new LinkedList<ChannelFuture>();

    // ipcGlobalTimer.cancel(); // shutdown timer tasks, take over all the controls.
    scheduledTaskExecutor.shutdown();
    synchronized (idChecker) {
      synchronized (disconnecter) {
        synchronized (recycler) {
          // make sure all the timer tasks are done.
          final Integer[] remoteIDs = channelPool.keySet().toArray(new Integer[] {});

          for (final Integer remoteID : remoteIDs) {
            removeRemote(remoteID);
          }

          allConnectionCloseFutures.add(serverCloseFuture);
          for (final Channel ch : allPossibleChannels) {
            allConnectionCloseFutures.add(ch.getCloseFuture());
          }
          new Thread() {
            @Override
            public void run() {
              while (allPossibleChannels.size() > 0) {
                idChecker.run();
                disconnecter.run();
                try {
                  Thread.sleep(100);
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                  e.printStackTrace();
                  break;
                }
              }
            }
          }.start();

          final DefaultChannelGroupFuture closeAll =
              new DefaultChannelGroupFuture(allPossibleChannels, allConnectionCloseFutures);
          closeAll.addListener(new ChannelGroupFutureListener() {

            @Override
            public void operationComplete(final ChannelGroupFuture future) throws Exception {
              final Thread resourceReleaser = new Thread() {
                @Override
                public void run() {
                  LOGGER.debug("pre release resources");
                  clientChannelFactory.releaseExternalResources();
                  serverChannel.getFactory().releaseExternalResources();
                  LOGGER.debug("post release resources");
                }
              };
              resourceReleaser.start();
            }
          });
          return closeAll;
        }
      }
    }
  }

  /**
   * start the pool service.
   * */
  public final void start() {
    clientChannelFactory =
        new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

    for (final Integer id : remoteAddresses.keySet()) {
      if (id != myID) {
        channelPool.put(id, new IPCRemote(id, remoteAddresses.get(id)));
      }
    }

    if (myID == 0) {
      serverBootstrap = ParallelUtility.createMasterIPCServer(messageQueue, this);
    } else {
      serverBootstrap = ParallelUtility.createWorkerIPCServer(messageQueue, this);
    }

    serverChannel = serverBootstrap.bind(myIPCServerAddress);

    allPossibleChannels.add(serverChannel);
    scheduledTaskExecutor.scheduleAtFixedRate(recycler,
        (int) ((0.5 + Math.random()) * CONNECTION_RECYCLE_INTERVAL_IN_MS), CONNECTION_RECYCLE_INTERVAL_IN_MS / 2,
        TimeUnit.MILLISECONDS);
    scheduledTaskExecutor.scheduleAtFixedRate(disconnecter,
        (int) ((0.5 + Math.random()) * CONNECTION_DISCONNECT_INTERVAL_IN_MS), CONNECTION_DISCONNECT_INTERVAL_IN_MS / 2,
        TimeUnit.MILLISECONDS);
    scheduledTaskExecutor.scheduleAtFixedRate(idChecker,
        (int) ((0.5 + Math.random()) * CONNECTION_ID_CHECK_TIMEOUT_IN_MS), CONNECTION_ID_CHECK_TIMEOUT_IN_MS / 2,
        TimeUnit.MILLISECONDS);
  }
}
