package edu.washington.escience.myriad.parallel;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * handler for control data
 */
@Sharable
public class WorkerDataHandler extends SimpleChannelUpstreamHandler {

  private static final Logger logger = Logger.getLogger(WorkerDataHandler.class.getName());

  LinkedBlockingQueue<MessageWrapper> messageBuffer;

  WorkerDataHandler(LinkedBlockingQueue<MessageWrapper> messageBuffer) {
    this.messageBuffer = messageBuffer;
  }

  @Override
  public void handleUpstream(final ChannelHandlerContext ctx, final ChannelEvent e) throws Exception {
    // if (e instanceof ChannelStateEvent) {
    // logger.info(e.toString());
    // }
    super.handleUpstream(ctx, e);
  }

  @Override
  public void channelOpen(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    super.channelOpen(ctx, e);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final TransportMessage tm = (TransportMessage) e.getMessage();
    Channel channel = e.getChannel();
    HashMap<String, Object> attributes = (HashMap<String, Object>) channel.getAttachment();
    final Integer senderID = (Integer) attributes.get("remoteID");
    final MessageWrapper mw = new MessageWrapper();
    mw.senderID = senderID;
    mw.message = tm;
    messageBuffer.add(mw);

    ctx.sendUpstream(e);
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final ExceptionEvent e) {
    logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
    e.getChannel().close();
  }
}