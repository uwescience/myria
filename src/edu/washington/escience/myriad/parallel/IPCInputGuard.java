package edu.washington.escience.myriad.parallel;

import java.nio.channels.ClosedChannelException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.DownstreamChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.UpstreamChannelStateEvent;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * This class monitors all the input/output IPC data. It makes sure that all input data are of {@link TransportMessage}
 * type. And it does all IPC exception catching and recording.
 * */
@Sharable
public class IPCInputGuard extends SimpleChannelHandler {

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(IPCInputGuard.class.getName());

  /**
   * constructor.
   * */
  public IPCInputGuard() {
  }

  @Override
  public final void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) {
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
    if (c != null) {
      c.close();
    }
  }

  @Override
  public final void handleDownstream(final ChannelHandlerContext ctx, final ChannelEvent e) throws Exception {
    if (e instanceof DownstreamChannelStateEvent) {
      final DownstreamChannelStateEvent ee = (DownstreamChannelStateEvent) e;
      switch (ee.getState()) {
        case OPEN:
        case BOUND:
        case INTEREST_OPS:
          break;
        case CONNECTED:
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Connection to remote. " + e.toString());
          }
          break;
      }
    }

    e.getFuture().addListener(new ChannelFutureListener() {

      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          if (LOGGER.isWarnEnabled()) {
            LOGGER.warn("Unknown downstream operation error: ChannelEvent: " + e + "; Cause: {}", future.getCause());
          }
        }
      }
    });
    super.handleDownstream(ctx, e);
  }

  @Override
  public final void handleUpstream(final ChannelHandlerContext ctx, final ChannelEvent e) throws Exception {
    if (e instanceof UpstreamChannelStateEvent) {
      final UpstreamChannelStateEvent ee = (UpstreamChannelStateEvent) e;
      switch (ee.getState()) {
        case OPEN:
        case BOUND:
        case INTEREST_OPS:
          break;
        case CONNECTED:
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Connection from remote. " + e.toString());
          }
          break;
      }
    }
    super.handleUpstream(ctx, e);
  }

  @Override
  public final void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final Object message = e.getMessage();
    if (!(message instanceof TransportMessage)) {
      throw new RuntimeException("Non-TransportMessage received: \n" + "\tfrom " + e.getRemoteAddress() + "\n"
          + "\tmessage: " + message);
    }

    ctx.sendUpstream(e);
  }
}
