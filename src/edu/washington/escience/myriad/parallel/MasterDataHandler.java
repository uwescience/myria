package edu.washington.escience.myriad.parallel;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

public class MasterDataHandler extends SimpleChannelUpstreamHandler {

  private static final Logger logger = Logger.getLogger(MasterDataHandler.class.getName());

  /**
   * messageBuffer.
   * */
  LinkedBlockingQueue<MessageWrapper> messageBuffer;

  /**
   * constructor.
   * */
  MasterDataHandler(final LinkedBlockingQueue<MessageWrapper> messageBuffer) {
    this.messageBuffer = messageBuffer;
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    // if (e instanceof ChannelStateEvent) {
    // logger.info(e.toString());
    // }
    super.handleUpstream(ctx, e);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    Channel channel = e.getChannel();
    TransportMessage tm = (TransportMessage) e.getMessage();
    HashMap<String, Object> attributes = (HashMap<String, Object>) channel.getAttachment();

    final Integer senderID = (Integer) attributes.get("remoteID");
    final MessageWrapper mw = new MessageWrapper();
    mw.senderID = senderID;
    mw.message = tm;
    messageBuffer.add(mw);
    ctx.sendUpstream(e);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    logger.log(Level.WARNING, "Unexpected exception from downstream.", e.getCause());
    e.getChannel().close();
  }

}