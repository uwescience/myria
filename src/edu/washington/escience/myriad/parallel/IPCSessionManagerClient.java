package edu.washington.escience.myriad.parallel;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;

import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;
import edu.washington.escience.myriad.util.IPCUtils;

@Sharable
public class IPCSessionManagerClient extends SimpleChannelHandler {

  private static final Logger logger = Logger.getLogger(IPCSessionManagerClient.class.getName());

  /**
   * Help the session management for ipc connection pool at IPC client.
   * */
  public IPCSessionManagerClient() {
  }

  /**
   * Invoked when something was written into a {@link Channel}.
   */
  @Override
  public void writeComplete(final ChannelHandlerContext ctx, final WriteCompletionEvent e) throws Exception {
    ChannelContext cs = ChannelContext.getChannelContext(e.getChannel());
    cs.updateLastIOTimestamp();

    ctx.sendUpstream(e);
  }

  /**
   * Invoked when {@link Channel#write(Object)} is called.
   */
  @Override
  public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    ChannelContext.getChannelContext(e.getChannel()).recordWriteFuture(e);
    ctx.sendDownstream(e);
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {

    Channel ch = e.getChannel();
    final TransportMessage tm = (TransportMessage) e.getMessage();
    ChannelContext cc = ChannelContext.getChannelContext(ch);
    ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();

    if (ecc == null) {
      // connect request sent from other workers
      Integer remoteID = IPCUtils.checkConnectTM(tm);
      if (remoteID != null) {
        cc.setRemoteReplyID(remoteID);
      } else {
        logger.log(Level.WARNING, "Channel: " + ch + ". Unknown session. Send me the remote id before data transfer.");
        throw new IllegalStateException("Unknown session. Send me the remote id before data transfer.");
      }
      return;
    }

    if (tm.getType() == TransportMessageType.DATA && tm.getData().getType() == DataMessage.DataMessageType.NORMAL) {
      // update io timestamp before data processing
      cc.updateLastIOTimestamp();
    }
    ctx.sendUpstream(e);

  }
}