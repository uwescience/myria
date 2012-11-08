package edu.washington.escience.myriad.parallel;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * handler for control data
 */
public class WorkerControlHandler extends SimpleChannelUpstreamHandler {

  private static final Logger logger = Logger.getLogger(WorkerControlHandler.class.getName());

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    // if (e instanceof ChannelStateEvent) {
    // logger.info(e.toString());
    // }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
    super.channelOpen(ctx, e);
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
    final TransportMessage tm = (TransportMessage) e.getMessage();

    if ((tm.getType() == TransportMessage.TransportMessageType.CONTROL)
        && (ControlMessage.ControlMessageType.CONNECT == tm.getControl().getType())) {
      // connect request sent from other workers
      final ControlMessage cm = tm.getControl();

      HashMap<String, Object> attributes = new HashMap<String, Object>();
      Channel channel = e.getChannel();
      channel.setAttachment(attributes);
      attributes.put("remoteID", cm.getRemoteID());
    }
    ctx.sendUpstream(e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
    e.getChannel().close();
  }
}