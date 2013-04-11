package edu.washington.escience.myriad.parallel.ipc;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.util.IPCUtils;

/**
 * Dealing with IPC session management at the server side.
 * */
@Sharable
public class IPCSessionManagerServer extends SimpleChannelHandler {

  /** The logger for this class. */
  private static final Logger LOGGER = Logger.getLogger(IPCSessionManagerServer.class.getName());

  /**
   * the IPC connection pool, this session manager serves to.
   * */
  private final IPCConnectionPool connectionPool;

  /**
   * Help the session management for ipc connection pool at IPC server.
   * 
   * @param connectionPool the IPC connection pool, this session manager serves to.
   * */
  public IPCSessionManagerServer(final IPCConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
  }

  @Override
  public final void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    final ChannelContext cs = ChannelContext.getChannelContext(e.getChannel());
    cs.connected();
    ctx.sendUpstream(e);
  }

  @Override
  public final void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    connectionPool.newAcceptedRemoteChannel(e.getChannel());
    ctx.sendUpstream(e);
  }

  @Override
  public final void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    final Channel ch = e.getChannel();
    final TransportMessage tm = (TransportMessage) e.getMessage();
    final ChannelContext att = ChannelContext.getChannelContext(ch);
    final ChannelContext.RegisteredChannelContext ecc = att.getRegisteredChannelContext();

    if (ecc == null) {
      // connect request sent from other workers
      final Integer remoteID = IPCUtils.checkConnectTM(tm);
      if (remoteID != null) {
        ch.write(connectionPool.getMyIDAsTM()).await(); // await to finish channel registering
        connectionPool.registerChannel(remoteID, ch);
      } else {
        LOGGER.log(Level.WARNING, "Channel: " + ch + ". Unknown session. Send me the remote id before data transfer.");
        throw new IllegalStateException("Unknown session. Send me the remote id before data transfer.");
      }
    } else {
      if (tm.getType() == TransportMessageType.DATA && tm.getData().getType() == DataMessage.DataMessageType.NORMAL) {
        // update io timestamp before data processing
        att.updateLastIOTimestamp();
      } else if (tm.getType() == TransportMessageType.CONTROL
          && tm.getControl().getType() == ControlMessage.ControlMessageType.DISCONNECT) {
        connectionPool.closeChannelRequested(ch);
        return;
      }
      ctx.sendUpstream(e);
    }
  }

  @Override
  public final void writeComplete(final ChannelHandlerContext ctx, final WriteCompletionEvent e) throws Exception {
    final ChannelContext cs = ChannelContext.getChannelContext(e.getChannel());
    cs.updateLastIOTimestamp();
    ctx.sendUpstream(e);
  }

  @Override
  public final void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    ChannelContext.getChannelContext(e.getChannel()).recordWriteFuture(e);
    ctx.sendDownstream(e);
  }
}