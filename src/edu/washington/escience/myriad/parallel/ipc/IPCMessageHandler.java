package edu.washington.escience.myriad.parallel.ipc;

import java.nio.channels.ClosedChannelException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
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
      cs.connected();
    }
    ctx.sendUpstream(e);
  }

  @Override
  public void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    if (ctx.getChannel().getParent() != null) {
      ownerConnectionPool.newAcceptedRemoteChannel(e.getChannel());
    }
    ctx.sendUpstream(e);
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
      LOGGER.error("Channel: " + ch + ". Unknown session. Send me the remote id before data transfer.");
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
                    "Duplicate BOS received from a stream channel. Existing Stream: (RemoteID:%1$s, StreamID:%2$d), new BOS: (RemoteID:%1$s, StreamID:%3$d). Dropped.",
                    remoteID, existingIChannel.getID().getStreamID(), streamID));
      } else {
        StreamIOChannelID ecID = new StreamIOChannelID(streamID, remoteID);
        StreamInputBuffer<Object> ib = ownerConnectionPool.getInputBuffer(ecID);
        if (ib == null) {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Unknown data stream: (RemoteID {}, stream ID:{}). Denined.", remoteID, streamID);
          }
          return;
        }
        StreamInputChannel<Object> ic = ib.getInputChannel(ecID);
        cc.getRegisteredChannelContext().getIOPair().mapInputChannel(ic);
      }
      return;
    } else if (metaMessage == IPCMessage.Meta.EOS) {
      if (existingIChannel == null) {
        LOGGER.error(String.format("EOS received from a non-stream channel. From RemoteID:%1$s.", remoteID));
      } else {
        receiveRegisteredData(ch, cc, IPCMessage.StreamData.eos(remoteID, cc.getRegisteredChannelContext().getIOPair()
            .getInputChannel().getID().getStreamID()));
        cc.getRegisteredChannelContext().getIOPair().deMapInputChannel();
      }
      return;
    } else if (metaMessage == IPCMessage.Meta.DISCONNECT) {
      if (existingIChannel != null) {
        LOGGER.error(String.format("DISCONNECT received when the channel is still in use as a stream input: %1$s.",
            existingIChannel.getID()));
      } else {
        if (ch.getParent() != null) {
          // serverChannel
          ownerConnectionPool.closeChannelRequested(ch);
        } else {
          if (LOGGER.isErrorEnabled()) {
            LOGGER.error("Disconnect should only be sent from client channel to accepted channel.");
          }
        }
      }
      return;
    } else if (metaMessage instanceof IPCMessage.Meta.CONNECT) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error("Duplicate Channel CONNECT message. Channel: {}, remoteID: {}. Dropped.", ch, remoteID);
      }
      return;
    }
  }

  /**
   * @param ch the source channel.
   * */
  private void receiveUnregisteredData(final Channel ch) {
    if (LOGGER.isErrorEnabled()) {
      LOGGER.error("Channel: {}. Unknown session. Send me the remote id before data transfer.", ch);
    }
  }

  /**
   * @param ch the source channel.
   * @param cc channel context of ch.
   * @param message the received message
   * */
  private void receiveRegisteredData(final Channel ch, final ChannelContext cc, final Object message) {
    cc.updateLastIOTimestamp();
    StreamIOChannelPair ecp = cc.getRegisteredChannelContext().getIOPair();
    final int remoteID = cc.getRegisteredChannelContext().getRemoteID();
    StreamInputChannel<Object> ic = ecp.getInputChannel();
    if (ic == null) {
      // connectionless message processing
      ShortMessageProcessor<Object> smp = ownerConnectionPool.getShortMessageProcessor();
      smp.processMessage(ch, IPCMessage.Data.wrap(remoteID, message));
    } else {
      while (!processStreamMessage(ch, remoteID, IPCMessage.StreamData
          .wrap(remoteID, ic.getID().getStreamID(), message))) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER
              .error("Input buffer out of memory. With the flow control input buffers, it should not happen normally.");
        }
      }
    }
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    final Channel ch = e.getChannel();
    Object msg = e.getMessage();
    if (msg instanceof ChannelBuffer) {
      // message from remote, deserialize
      ChannelBuffer cb = (ChannelBuffer) msg;
      msg = IPCMessage.Meta.deSerialize(cb);
      if (msg == null) {
        // user message
        final ChannelContext cc = ChannelContext.getChannelContext(ch);
        final int remoteID = cc.getRegisteredChannelContext().getRemoteID();

        StreamInputChannel<?> ic = cc.getRegisteredChannelContext().getIOPair().getInputChannel();
        if (ic != null) {
          // it's a stream message
          StreamInputBuffer<?> sib = ic.getInputBuffer();
          msg = ownerConnectionPool.getPayloadSerializer().deSerialize(cb, sib.getProcessor(), sib.getAttachment());
          if (msg == null) {
            LOGGER.error("Unknown stream message from {} to {}, msg: {}", remoteID, sib.getProcessor(), cb);
            return;
          }
        } else {
          // short message
          msg =
              ownerConnectionPool.getPayloadSerializer().deSerialize(cb, null,
                  ownerConnectionPool.getShortMessageProcessor().getAttachment());
          if (msg == null) {
            LOGGER.error("Unknown short message from {}, msg: {}", remoteID, cb);
            return;
          }
        }
      }
    }

    final ChannelContext cc = ChannelContext.getChannelContext(ch);
    final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();

    if (msg instanceof IPCMessage.Meta) {
      // process meta messates
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
      if (ecc == null) {
        receiveUnregisteredData(ch);
      } else {
        receiveRegisteredData(ch, cc, msg);
      }
    }
    ctx.sendUpstream(e);
  }

  /**
   * @param ch source channel.
   * @param remoteID source remote.
   * @param message the message.
   * @return true if successfully processed.
   * */
  private boolean processStreamMessage(final Channel ch, final int remoteID, final IPCMessage.StreamData<Object> message) {
    boolean pushToBufferSucceed = true;

    final ChannelContext cs = (ChannelContext) ch.getAttachment();
    StreamIOChannelPair ecp = cs.getRegisteredChannelContext().getIOPair();
    StreamInputChannel<Object> cc = ecp.getInputChannel();
    StreamInputBuffer<Object> msgDestIB = cc.getInputBuffer();

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
    String errorMessage = cause.getMessage();
    if (errorMessage == null) {
      errorMessage = "";
    }
    if (cause instanceof java.nio.channels.NotYetConnectedException) {
      LOGGER.warn("Channel " + c + ": not yet connected. " + errorMessage, cause);
    } else if (cause instanceof java.net.ConnectException) {
      LOGGER.warn("Channel " + c + ": Connection failed: " + errorMessage, cause);
    } else if (cause instanceof java.io.IOException && errorMessage.contains("reset by peer")) {
      LOGGER.warn("Channel " + c + ": Connection reset by peer: " + c.getRemoteAddress() + " " + errorMessage, cause);
    } else if (cause instanceof ClosedChannelException) {
      LOGGER.warn("Channel " + c + ": Connection reset by peer: " + c.getRemoteAddress() + " " + errorMessage, cause);
    } else {
      LOGGER.warn("Channel " + c + ": Unexpected exception from downstream.", cause);
    }
    ChannelContext cc = ChannelContext.getChannelContext(c);
    if (cc != null) {
      StreamIOChannelPair pair = cc.getRegisteredChannelContext().getIOPair();
      pair.deMapInputChannel();
      pair.deMapOutputChannel();
    }
    ownerConnectionPool.errorEncountered(c, cause);
  }

  @Override
  public void writeComplete(final ChannelHandlerContext ctx, final WriteCompletionEvent e) throws Exception {
    final ChannelContext cs = ChannelContext.getChannelContext(e.getChannel());
    cs.updateLastIOTimestamp();
    ctx.sendUpstream(e);
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
            ChannelBuffers.wrappedBuffer(IPCMessage.Data.SERIALIZE_HEAD, ownerConnectionPool.getPayloadSerializer()
                .serialize(m));
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

    if (!ioChannel.isReadable()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Channel {} is changed to be unreadable.", ioChannel);
      }
    }
    ctx.sendUpstream(e);
  }
}