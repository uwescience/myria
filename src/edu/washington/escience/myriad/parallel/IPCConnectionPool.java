package edu.washington.escience.myriad.parallel;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

/**
 * Keep a pool of n connections, indexed by 0~n-1
 */
public class IPCConnectionPool {

  protected final ConcurrentHashMap<Integer, AtomicReference<Channel>> connectionPool;
  protected final ConcurrentHashMap<Integer, SocketInfo> remoteAddresses;

  /**
   * NIO client side factory.
   * */
  protected final ChannelFactory clientFactory;
  protected final ChannelPipelineFactory channelPipelineFactory;

  protected final int myID;
  protected final ChannelGroup allConnections;

  public IPCConnectionPool(final int myID, final Map<Integer, SocketInfo> remoteAddresses,
      final LinkedBlockingQueue<MessageWrapper> messageQueue) {

    connectionPool = new ConcurrentHashMap<Integer, AtomicReference<Channel>>();
    for (final Integer id : remoteAddresses.keySet()) {
      connectionPool.put(id, new AtomicReference<Channel>());
    }
    this.remoteAddresses = new ConcurrentHashMap<Integer, SocketInfo>();
    this.remoteAddresses.putAll(remoteAddresses);

    this.myID = myID;
    allConnections = new DefaultChannelGroup("allChannels");
    clientFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    channelPipelineFactory = new IPCPipelineFactories.WorkerClientPipelineFactory(messageQueue);
  }

  /**
   * if ioHandler is null, the default ioHandler will be used
   * 
   * @param id, identity of remote unit
   */
  @SuppressWarnings("unchecked")
  public final Channel get(final int id, final int numRetry, final Map<String, ?> connectionAttributes) {

    final AtomicReference<Channel> ref = connectionPool.get(id);

    Channel s = null;
    HashMap<String, Object> attributes = null;
    int retry = 0;
    Channel oldChannel = ref.get();

    // Not a bug
    synchronized (ref) {

      while (retry < numRetry && ((s = ref.get()) == null || !s.isConnected())) {
        final Channel old = s;
        s = createChannel(remoteAddresses.get(id).getAddress(), 3000);
        ref.compareAndSet(old, s);
        retry++;
      }

      if ((s = ref.get()) == null || !s.isConnected()) {
        // fail to connect
        return null;
      }

      if (oldChannel != s) {
        // this is a new connection
        synchronized (allConnections) {
          allConnections.add(s);
        }

        attributes = new HashMap<String, Object>();
        s.setAttachment(attributes);
        attributes.put("remoteId", id);
        s.write(
            TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
                ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.CONNECT).setRemoteID(myID)
                    .build()).build()).awaitUninterruptibly();
      } else {
        attributes = (HashMap<String, Object>) s.getAttachment();
      }

      if (connectionAttributes != null) {
        for (final Entry<String, ?> attribute : connectionAttributes.entrySet()) {
          String key = attribute.getKey();
          if (!key.equals("remoteId")) {
            attributes.put(attribute.getKey(), attribute.getValue());
          }
        }
      }
    }
    return s;

  }

  public void release(final int i) {
    // TODO
  }

  public void shutdown() {
    synchronized (allConnections) {
      allConnections.disconnect().awaitUninterruptibly();
      allConnections.close().awaitUninterruptibly();
    }
  }

  public void start() {
    // TODO
  }

  /**
   * Connect to remoteAddress with timeout connectionTimeoutMS.
   * 
   * @return the nio channel if successful, null otherwise.
   * */
  private Channel createChannel(final SocketAddress remoteAddress, final long connectionTimeoutMS) {

    Channel channel = null;

    ClientBootstrap ic = null;
    ic = ParallelUtility.createIPCClient(clientFactory, channelPipelineFactory);

    boolean connected = true;
    ChannelFuture c = null;
    try {
      c = ic.connect(remoteAddress);
    } catch (Exception e) {
      connected = false;
    }
    if (connected) {
      if (connectionTimeoutMS > 0) {
        connected = c.awaitUninterruptibly(connectionTimeoutMS);
      } else {
        connected = c.awaitUninterruptibly().isSuccess();
      }
    }
    if (connected) {
      channel = c.getChannel();
      return channel;
    }
    return null;
  }
}
