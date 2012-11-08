package edu.washington.escience.myriad.parallel;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

public class IPCInputGuard extends SimpleChannelUpstreamHandler {

  private static final Logger logger = Logger.getLogger(MasterDataHandler.class.getName());

  /**
   * constructor.
   * */
  IPCInputGuard() {
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    if (e instanceof ChannelStateEvent) {
      logger.info(e.toString());
    }
    super.handleUpstream(ctx, e);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    Object message = e.getMessage();
    if (!(message instanceof TransportMessage)) {
      throw new RuntimeException("Non-TransportMessage received: \n" + "\tfrom " + e.getRemoteAddress() + "\n"
          + "\tmessage: " + message);
    }

    ctx.sendUpstream(e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    Channel c = e.getChannel();
    Throwable cause = e.getCause();
    if (cause instanceof java.nio.channels.NotYetConnectedException) {
      logger.log(Level.WARNING, "Channel not yet connected. " + cause.getMessage());
      return;
    }
    if (cause instanceof java.net.ConnectException) {
      logger.log(Level.WARNING, "Connection failed: " + cause.getMessage());
      return;
    }
    if (cause instanceof java.io.IOException && cause.getMessage().contains("reset by peer")) {
      logger.log(Level.WARNING, "Connection reset by peer: " + c.getRemoteAddress() + " " + cause.getMessage());
      return;
    }
    logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
    logger.log(Level.WARNING, "Error channel: " + e.getChannel());
    if (c != null) {
      c.close();
    }
  }
}
