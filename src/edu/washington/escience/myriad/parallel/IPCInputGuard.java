package edu.washington.escience.myriad.parallel;

import java.nio.channels.ClosedChannelException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DownstreamChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamChannelStateEvent;

import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

@Sharable
public class IPCInputGuard extends SimpleChannelHandler {

  private static final Logger logger = Logger.getLogger(IPCInputGuard.class.getName());

  /**
   * constructor.
   * */
  IPCInputGuard() {
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
      logger.log(Level.WARNING, "Channel " + c + ": not yet connected. " + errorMessage, cause);
    } else if (cause instanceof java.net.ConnectException) {
      logger.log(Level.WARNING, "Channel " + c + ": Connection failed: " + errorMessage, cause);
    } else if (cause instanceof java.io.IOException && errorMessage.contains("reset by peer")) {
      logger.log(Level.WARNING, "Channel " + c + ": Connection reset by peer: " + c.getRemoteAddress() + " "
          + errorMessage, cause);
    } else if (cause instanceof ClosedChannelException) {
      logger.log(Level.WARNING, "Channel " + c + ": Connection reset by peer: " + c.getRemoteAddress() + " "
          + errorMessage, cause);
    } else {
      logger.log(Level.WARNING, "Channel " + c + ": Unexpected exception from downstream.", cause);
    }
    if (c != null) {
      c.close();
    }
  }

  @Override
  public void handleDownstream(final ChannelHandlerContext ctx, final ChannelEvent e) throws Exception {
    if (e instanceof DownstreamChannelStateEvent) {
      final DownstreamChannelStateEvent ee = (DownstreamChannelStateEvent) e;
      switch (ee.getState()) {
        case OPEN:
        case BOUND:
          break;
        case CONNECTED:
          logger.info("Connection to remote. " + e.toString());
          break;
      }
    }
    super.handleDownstream(ctx, e);
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx, final ChannelEvent e) throws Exception {
    if (e instanceof UpstreamChannelStateEvent) {
      final UpstreamChannelStateEvent ee = (UpstreamChannelStateEvent) e;
      switch (ee.getState()) {
        case OPEN:
        case BOUND:
          break;
        case CONNECTED:
          logger.info("Connection from remote. " + e.toString());
          break;
      }
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final Object message = e.getMessage();
    if (!(message instanceof TransportMessage)) {
      throw new RuntimeException("Non-TransportMessage received: \n" + "\tfrom " + e.getRemoteAddress() + "\n"
          + "\tmessage: " + message);
    }

    ctx.sendUpstream(e);
  }
}
