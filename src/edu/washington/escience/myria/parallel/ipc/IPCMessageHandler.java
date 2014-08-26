package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.channel.local.LocalChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.parallel.ipc.ChannelContext.RegisteredChannelContext;
import edu.washington.escience.myria.util.concurrent.ThreadStackDump;

/**
 * Dealing with IPC IO messages.
 * */
@Sharable
public final class IPCMessageHandler extends SimpleChannelHandler {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(IPCMessageHandler.class);

  /**
   * the IPC connection pool, this session manager serves to.
   * */
  private final IPCConnectionPool ownerConnectionPool;

  /**
   * Help the session management for ipc connection pool at IPC server.
   *
   * @param connectionPool the IPC connection pool, this session manager serves to.
   * */
  public IPCMessageHandler(final IPCConnectionPool connectionPool) {
    ownerConnectionPool = connectionPool;
  }

  @Override
  public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    if (ctx.getChannel().getParent() != null) {
      final ChannelContext cs = ChannelContext.getChannelContext(e.getChannel());
      if (cs != null) {
        cs.connected();
      }
    }
  }

  @Override
  public void channelDisconnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    ownerConnectionPool.channelDisconnected(ctx.getChannel());
  }

  @Override
  public void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    if (ctx.getChannel().getParent() != null) {
      ownerConnectionPool.newAcceptedRemoteChannel(e.getChannel());
    }
  }

  /**
   * @param ch the source channel.
   * @param cc channel context of ch.
   * @param metaMessage the received message
   * @throws InterruptedException if interrupted.
   * */
  private void receiveUnregisteredMeta(final Channel ch, final ChannelContext cc, final IPCMessage.Meta metaMessage)
      throws InterruptedException {
    if (metaMessage instanceof IPCMessage.Meta.CONNECT) {
      int remoteID = ((IPCMessage.Meta.CONNECT) metaMessage).getRemoteID();
      if (!ownerConnectionPool.isRemoteValid(remoteID)) {
        throw new ChannelException("Unknown RemoteID: " + remoteID);
      }
      if (ch.getParent() != null) {
        // server channel
        ch.write(ownerConnectionPool.getMyIDAsMsg()).await(); // await to finish channel registering
        ownerConnectionPool.registerChannel(remoteID, ch);
      } else {
        // client channel
        cc.setRemoteReplyID(remoteID);
      }
      return;
    }

    if (LOGGER.isErrorEnabled()) {
      LOGGER.error("Channel: {}. Unknown session. Send me the remote id before data transfer.", ChannelContext
          .channelToString(ch));
    }
  }

  /**
   * @param ch the source channel.
   * @param cc channel context of ch.
   * @param metaMessage the received message
   * */
  private void receiveRegisteredMeta(final Channel ch, final ChannelContext cc, final IPCMessage.Meta metaMessage) {
    int remoteID = cc.getRegisteredChannelContext().getRemoteID();
    StreamInputChannel<Object> existingIChannel = cc.getRegisteredChannelContext().getIOPair().getInputChannel();

    if (metaMessage instanceof IPCMessage.Meta.BOS) {
      // At the beginning of a stream, record the operator id.
      final long streamID = ((IPCMessage.Meta.BOS) metaMessage).getStreamID();
      if (existingIChannel != null) {
        LOGGER
            .error(String
                .format(
                    "Duplicate BOS received from a stream channel %4$s. Existing Stream: (RemoteID:%1$s, StreamID:%2$d), new BOS: (RemoteID:%1$s, StreamID:%3$d). Dropped.",
                    remoteID, existingIChannel.getID().getStreamID(), streamID, ChannelContext.channelToString(ch)));
      } else {
        StreamIOChannelID ecID = new StreamIOChannelID(streamID, remoteID);
        StreamInputBuffer<Object> ib = ownerConnectionPool.getInputBuffer(ecID);
        if (ib == null) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Unknown data stream: (RemoteID {}, stream ID:{}). Received through {}. Denined.", remoteID,
                streamID, ChannelContext.channelToString(ch));
          }
          return;
        }
        StreamInputChannel<Object> ic = ib.getInputChannel(ecID);
        cc.getRegisteredChannelContext().getIOPair().mapInputChannel(ic);
      }
      return;
    } else if (metaMessage == IPCMessage.Meta.EOS) {
      if (existingIChannel == null) {
        LOGGER.error("EOS received from a non-stream channel {}. From RemoteID:{}.",
            ChannelContext.channelToString(ch), remoteID);
      } else {
        long streamID = cc.getRegisteredChannelContext().getIOPair().getInputChannel().getID().getStreamID();
        receiveRegisteredStreamData(cc.getRegisteredChannelContext().getIOPair().getInputChannel(),
            IPCMessage.StreamData.eos(remoteID, streamID));
        cc.getRegisteredChannelContext().getIOPair().deMapInputChannel();
        ChannelContext.resumeRead(ch);
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace("EOS received from physical channel {} Logical channel is opID:{},rmtID{}.", ChannelContext
              .channelToString(ch), streamID, remoteID);
        }
      }
      return;
    } else if (metaMessage == IPCMessage.Meta.DISCONNECT) {
      if (existingIChannel != null) {
        LOGGER.error(
            "DISCONNECT received when the channel is still in use as a stream input: {}. Physical channel: {}",
            existingIChannel.getID(), ChannelContext.channelToString(ch));
      } else {
        if (ch.getParent() != null) {
          // serverChannel
          ownerConnectionPool.closeChannelRequested(ch);
        } else {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error(
                "Disconnect should only be sent from client channel to accepted channel. Physical channel: {}",
                ChannelContext.channelToString(ch));
          }
        }
      }
      return;
    } else if (metaMessage instanceof IPCMessage.Meta.CONNECT) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Duplicate Channel CONNECT message. Channel: {}, remoteID: {}. Dropped.", ChannelContext
            .channelToString(ch), remoteID);
      }
      return;
    }
  }

  /**
   * @param sic stream input channel.
   * @param message the received message
   * */
  private void receiveRegisteredStreamData(final StreamInputChannel<Object> sic,
      final IPCMessage.StreamData<Object> message) {
    while (!processStreamMessage(sic, message)) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Input buffer out of memory. With the flow control input buffers, it should not happen normally.");
      }
    }
  }

  /**
   * Deserialize messages received from remote.
   *
   * @param ch the source channel
   * @param cb the serialized data buffer
   * @return deserialized {@link IPCMessage}
   * @throws Exception if any error occurs
   * */
  private IPCMessage deSerializeIPCMessage(final Channel ch, final ChannelBuffer cb) throws Exception {
    // message from remote, deserialize
    IPCMessage msg = null;
    msg = IPCMessage.Meta.deSerialize(cb);
    if (msg == null) {
      // user message
      final ChannelContext cc = ChannelContext.getChannelContext(ch);
      final int remoteID = cc.getRegisteredChannelContext().getRemoteID();

      if (IPCMessage.StreamData.isStreamData(cb)) {
        // it's a stream message
        StreamInputChannel<?> ic = cc.getRegisteredChannelContext().getIOPair().getInputChannel();
        if (ic == null) {
          LOGGER.error("Stream message received from channel {}. Destination unknown. Drop it.", ChannelContext
              .channelToString(ch));
        } else {
          StreamInputBuffer<?> sib = ic.getInputBuffer();
          Object usrMsg =
              ownerConnectionPool.getPayloadSerializer().deSerialize(cb, sib.getProcessor(), sib.getAttachment());
          if (usrMsg == null) {
            LOGGER.error("Unknown stream message type from {} to {}, drop it.", remoteID, sib.getProcessor());
          } else {
            msg = IPCMessage.StreamData.wrap(remoteID, ic.getID().getStreamID(), usrMsg);
          }
        }
      } else if (IPCMessage.Msg.isShortMsg(cb)) {
        // short message
        Object usrMsg =
            ownerConnectionPool.getPayloadSerializer().deSerialize(cb, null,
                ownerConnectionPool.getShortMessageProcessor().getAttachment());
        if (usrMsg == null) {
          LOGGER.error("Unknown short message type from {}, drop it.", remoteID);
        } else {
          msg = IPCMessage.Msg.wrap(remoteID, usrMsg);
        }
      } else {
        LOGGER.error("Unknown IPC message type received from {}.", ChannelContext.channelToString(ch));
      }
    }
    return msg;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    final Channel ch = e.getChannel();
    Object msgObj = e.getMessage();
    IPCMessage msg = null;
    if (msgObj instanceof ChannelBuffer) {
      msg = deSerializeIPCMessage(ch, (ChannelBuffer) msgObj);

    } else if (msgObj instanceof IPCMessage) {
      msg = (IPCMessage) msgObj;
    } else {
      LOGGER.error("Received unknown msg type: {}, from channel {}", msgObj.getClass().getName(), ChannelContext
          .channelToString(ch));
    }

    if (msg == null) {
      return;
    }

    final ChannelContext cc = ChannelContext.getChannelContext(ch);

    if (LOGGER.isTraceEnabled()) {
      if (msg instanceof IPCMessage.Meta) {
        LOGGER.trace("Received meta msg: {}, from channel {}", msg, ChannelContext.channelToString(ch));
      } else if (msg instanceof IPCMessage.StreamData) {
        LOGGER.trace("Received user stream data of type: {}, from channel {}", ((IPCMessage.StreamData) msg)
            .getPayload().getClass().getName(), ChannelContext.channelToString(ch));
      } else if (msg instanceof IPCMessage.Msg) {
        LOGGER.trace("Received user short message of type: {}, from channel {}", ((IPCMessage.Msg) msg).getPayload()
            .getClass().getName(), ChannelContext.channelToString(ch));
      }
    }

    if (cc != null) {
      final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();

      if (msg instanceof IPCMessage.Meta) {
        // process meta messages
        if (msg == IPCMessage.Meta.PING) {
          // IPC ping, drop directly.
          return;
        }
        if (ecc == null) {
          receiveUnregisteredMeta(ch, cc, (IPCMessage.Meta) msg);
        } else {
          receiveRegisteredMeta(ch, cc, (IPCMessage.Meta) msg);
        }
      } else {
        if (ecc != null) {
          cc.updateLastIOTimestamp();
          if (msg instanceof IPCMessage.StreamData) {
            StreamInputChannel<Object> sic = cc.getRegisteredChannelContext().getIOPair().getInputChannel();
            receiveRegisteredStreamData(sic, (IPCMessage.StreamData) msg);
          } else if (msg instanceof IPCMessage.Msg) {
            // connectionless message processing
            ShortMessageProcessor<Object> smp = ownerConnectionPool.getShortMessageProcessor();
            smp.processMessage(ch, (IPCMessage.Msg) msg);
          } else {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Unknown message type: {} received from channel {}.", msg.getClass().getName(),
                  ChannelContext.channelToString(ch));
            }
          }
        } else {
          LOGGER.error("Channel: {}. Unknown session. Send me the remote id before data transfer.", ChannelContext
              .channelToString(ch));
        }
      }
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER
            .debug("Channel context is null. The IPCConnectionPool must have been shutdown. Close the channel directly.");
      }
      ch.close();
    }
  }

  /**
   * @param sic stream input channel.
   * @param message the message.
   * @return true if successfully processed.
   * */
  private boolean processStreamMessage(final StreamInputChannel<Object> sic, final IPCMessage.StreamData<Object> message) {
    boolean pushToBufferSucceed = true;
    StreamInputBuffer<Object> msgDestIB = sic.getInputBuffer();

    if (msgDestIB == null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Drop Data messge because the destination operator already ends.: {}", message);
      }
      return true;
    }

    pushToBufferSucceed = msgDestIB.offer(message);
    return pushToBufferSucceed;
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) {
    final Channel c = e.getChannel();
    final Throwable cause = e.getCause();
    ChannelContext cc = ChannelContext.getChannelContext(c);
    if (cc != null) {
      LOGGER.warn("Error occur in managed Netty Channel: {}, deregistering.", ChannelContext.channelToString(c), cause);
      RegisteredChannelContext rcc = cc.getRegisteredChannelContext();
      if (rcc != null) {
        StreamIOChannelPair pair = rcc.getIOPair();
        pair.deMapInputChannel();
        pair.deMapOutputChannel();
      }
      ownerConnectionPool.errorEncountered(c, cause);
    } else {
      LOGGER.warn("Unknown error occur in unmanaged Netty Channel: {}, close directly.", ChannelContext
          .channelToString(c), cause);
      c.close();
    }
  }

  @Override
  public void writeComplete(final ChannelHandlerContext ctx, final WriteCompletionEvent e) throws Exception {
    final ChannelContext cs = ChannelContext.getChannelContext(e.getChannel());
    cs.updateLastIOTimestamp();
  }

  @Override
  public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    Channel ch = e.getChannel();
    ChannelContext cc = ChannelContext.getChannelContext(ch);
    cc.recordWriteFuture(e);

    if (ch instanceof LocalChannel) {
      // local channels do no serialization
      ctx.sendDownstream(e);
    } else {
      // remote channels do serialization
      Object m = e.getMessage();
      ChannelBuffer codedMsg = null;
      if (m instanceof IPCMessage.Meta) {
        codedMsg = ((IPCMessage.Meta) m).serialize();
      } else {
        /*
         * m could be: 1. a TupleBatch (corresponds to IPCMessage.StreamData), 2. TransportMessage.QUERY or a
         * TransportMessage.CONTROL (corresponds to IPCMessage.Data but not StreamData). In both cases m is going to be
         * serialized as an IPCMessage.Data, with the header.
         */
        codedMsg =
            ChannelBuffers.wrappedBuffer(IPCMessage.StreamData.SERIALIZE_HEAD, ownerConnectionPool
                .getPayloadSerializer().serialize(m));
      }
      ctx.sendDownstream(new DownstreamMessageEvent(ch, e.getFuture(), codedMsg, e.getRemoteAddress()));
    }
  }

  @Override
  public void channelInterestChanged(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    Channel ioChannel = ctx.getChannel();
    StreamIOChannelPair p = ChannelContext.getChannelContext(ioChannel).getRegisteredChannelContext().getIOPair();
    StreamOutputChannel<?> oc = p.getOutputChannel();
    if (oc != null) {
      oc.channelInterestChangedCallback();
    }

    if (LOGGER.isTraceEnabled()) {
      String v = "readable";
      if (!ioChannel.isReadable()) {
        v = "unreadable";
      }
      LOGGER.trace("Channel {} is changed to be {}.", ChannelContext.channelToString(ioChannel), v,
          new ThreadStackDump());
    }
  }
}