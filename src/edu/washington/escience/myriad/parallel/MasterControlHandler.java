package edu.washington.escience.myriad.parallel;

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import edu.washington.escience.myriad.proto.ControlProto.ControlMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

@Sharable
public class MasterControlHandler extends SimpleChannelUpstreamHandler {

  private static final Logger logger = Logger.getLogger(MasterControlHandler.class.getName());

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) {
    logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
    e.getChannel().close();
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx, final ChannelEvent e) throws Exception {
    super.handleUpstream(ctx, e);
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {

    final TransportMessage tm = (TransportMessage) e.getMessage();
    final Channel channel = e.getChannel();
    if ((tm.getType() == TransportMessage.TransportMessageType.CONTROL)
        && (ControlMessage.ControlMessageType.CONNECT == tm.getControl().getType())) {
      // connect request sent from other workers
      final ControlMessage cm = tm.getControl();
      final HashMap<String, Object> attributes = new HashMap<String, Object>();
      channel.setAttachment(attributes);
      attributes.put("remoteID", cm.getRemoteID());
    }
    ctx.sendUpstream(e);
  }
}