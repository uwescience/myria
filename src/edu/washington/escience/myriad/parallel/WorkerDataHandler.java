package edu.washington.escience.myriad.parallel;

import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.parallel.Worker.MessageWrapper;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;

/**
 * handler for control data
 */
@Sharable
public class WorkerDataHandler extends SimpleChannelUpstreamHandler {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerDataHandler.class.getName());

  LinkedBlockingQueue<MessageWrapper> messageQueue;

  WorkerDataHandler(final LinkedBlockingQueue<MessageWrapper> messageQueue) {
    this.messageQueue = messageQueue;
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final TransportMessage tm = (TransportMessage) e.getMessage();
    final Channel channel = e.getChannel();
    final ChannelContext cs = (ChannelContext) channel.getAttachment();
    final ChannelContext.RegisteredChannelContext ecc = cs.getRegisteredChannelContext();
    final Integer senderID = ecc.getRemoteID();
    LOGGER.debug("received a message from " + senderID);
    final MessageWrapper mw = new MessageWrapper(senderID, tm);
    messageQueue.add(mw);

    ctx.sendUpstream(e);
  }

}