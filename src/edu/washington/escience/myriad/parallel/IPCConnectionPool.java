package edu.washington.escience.myriad.parallel;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

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
  // protected final ConcurrentHashMap<Integer, ChannelPipelineFactory> defaultIoHandlers;

  protected final int myID;
  protected final ChannelGroup allChannels;

  public IPCConnectionPool(final int myID, final Map<Integer, SocketInfo> remoteAddresses) {
    // final Map<Integer, ChannelPipelineFactory> defaultIoHandlers) {
    connectionPool = new ConcurrentHashMap<Integer, AtomicReference<Channel>>();
    for (final Integer id : remoteAddresses.keySet()) {
      connectionPool.put(id, new AtomicReference<Channel>());
    }
    this.remoteAddresses = new ConcurrentHashMap<Integer, SocketInfo>();
    this.remoteAddresses.putAll(remoteAddresses);
    // this.defaultIoHandlers = new ConcurrentHashMap<Integer, ChannelPipelineFactory>();
    // this.defaultIoHandlers.putAll(defaultIoHandlers);
    this.myID = myID;
    allChannels = new DefaultChannelGroup("allChannels");
  }

  /**
   * if ioHandler is null, the default ioHandler will be used
   */
  @SuppressWarnings("unchecked")
  public Channel get(final int id, final int numRetry, final Map<String, ?> connectionAttributes,
      final LinkedBlockingQueue<MessageWrapper> messageBuffer) {

    final AtomicReference<Channel> ref = connectionPool.get(id);

    Channel s = null;
    HashMap<String, Object> attributes = null;

    // Not a bug
    synchronized (ref) {

      int retry = 0;
      Channel oldChannel = ref.get();
      while (retry < numRetry && ((s = ref.get()) == null || !s.isConnected())) {
        final Channel old = s;
        s = createChannel(remoteAddresses.get(id).getAddress(), 3000, messageBuffer);
        ref.compareAndSet(old, s);
        retry++;
      }

      if ((s = ref.get()) == null || !s.isConnected()) {
        // fail to connect
        return null;
      }

      if (oldChannel != s) {
        synchronized (allChannels) {
          allChannels.add(s);
        }

        attributes = new HashMap<String, Object>();
        s.setAttachment(attributes);

        if (attributes.get("remoteId") == null) {
          attributes.put("remoteId", id);
          s.write(
              TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
                  ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.CONNECT).setRemoteID(myID)
                      .build()).build()).awaitUninterruptibly();
        }
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
    synchronized (allChannels) {
      allChannels.disconnect().awaitUninterruptibly();
      allChannels.close().awaitUninterruptibly();
    }
  }

  public void start() {
    // TODO
  }

  private static Channel createChannel(final SocketAddress remoteAddress, final long connectionTimeoutMS,
      final LinkedBlockingQueue<MessageWrapper> messageBuffer) {

    Channel channel = null;

    ClientBootstrap ic = null;
    ic = ParallelUtility.createConnector(messageBuffer);
    // ic.setHandler(ioHandler);

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
