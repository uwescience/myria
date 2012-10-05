package edu.washington.escience.myriad.parallel;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IoSession;

import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

/**
 * Keep a pool of n connections, indexed by 0~n-1
 * */
public class ConnectionPool {

  protected final ConcurrentHashMap<Integer, AtomicReference<IoSession>> sessionPool;
  protected final ConcurrentHashMap<Integer, SocketInfo> remoteAddresses;
  protected final ConcurrentHashMap<Integer, IoHandler> defaultIoHandlers;

  protected final int myID;

  public ConnectionPool(final int myID, final Map<Integer, SocketInfo> remoteAddresses, final Map<Integer, IoHandler> defaultIoHandlers) {
    this.sessionPool = new ConcurrentHashMap<Integer, AtomicReference<IoSession>>();
    for (final Integer id : remoteAddresses.keySet()) {
      this.sessionPool.put(id, new AtomicReference<IoSession>());
    }
    this.remoteAddresses = new ConcurrentHashMap<Integer, SocketInfo>();
    this.remoteAddresses.putAll(remoteAddresses);
    this.defaultIoHandlers = new ConcurrentHashMap<Integer, IoHandler>();
    this.defaultIoHandlers.putAll(defaultIoHandlers);
    this.myID = myID;
  }

  /**
   * if ioHandler is null, the default ioHandler will be used
   * */
  public IoSession get(final int id, IoHandler ioHandler, final int numRetry, final Map<String, ?> sessionAttributes) {

    final AtomicReference<IoSession> ref = sessionPool.get(id);
    IoSession s = null;
    if (ioHandler == null) {
      ioHandler = defaultIoHandlers.get(id);
    }

    final int retry = 0;
    while (retry < numRetry && ((s = ref.get()) == null || s.isClosing())) {
      final IoSession old = s;
      s = ParallelUtility.createSession(remoteAddresses.get(id).getAddress(), ioHandler, 3000);
      ref.compareAndSet(old, s);
    }
    if (s.getAttribute("remoteId") == null) {
      s.setAttribute("remoteId", id);
      s.write(
          TransportMessage.newBuilder().setType(TransportMessageType.CONTROL).setControl(
              ControlMessage.newBuilder().setType(ControlMessage.ControlMessageType.CONNECT).setRemoteID(myID).build())
              .build()).awaitUninterruptibly();
    }

    if (sessionAttributes != null) {
      for (final Entry<String, ?> attribute : sessionAttributes.entrySet()) {
        s.setAttribute(attribute.getKey(), attribute.getValue());
      }
    }
    return s;
  }

  public void release(final int i) {
    // TODO
  }

  public void shutdown() {
    // TODO
  }

  public void start() {
    // TODO
  }
}
