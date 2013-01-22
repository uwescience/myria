package edu.washington.escience.myriad.parallel;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
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

  LinkedBlockingQueue<MessageWrapper> messageQueue;

  WorkerDataHandler(LinkedBlockingQueue<MessageWrapper> messageQueue) {
    this.messageQueue = messageQueue;
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final TransportMessage tm = (TransportMessage) e.getMessage();
    Channel channel = e.getChannel();
    ChannelContext cs = (ChannelContext) channel.getAttachment();
    ChannelContext.RegisteredChannelContext ecc = cs.getRegisteredChannelContext();
    final Integer senderID = ecc.getRemoteID();
    final MessageWrapper mw = new MessageWrapper();
    mw.senderID = senderID;
    mw.message = tm;
    messageQueue.add(mw);

    ctx.sendUpstream(e);
  }

}