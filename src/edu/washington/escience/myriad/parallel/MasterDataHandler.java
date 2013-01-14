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

@Sharable
public class MasterDataHandler extends SimpleChannelUpstreamHandler {

  private static final Logger logger = Logger.getLogger(MasterDataHandler.class.getName());

  /**
   * messageQueue.
   * */
  LinkedBlockingQueue<MessageWrapper> messageQueue;

  /**
   * constructor.
   * */
  MasterDataHandler(final LinkedBlockingQueue<MessageWrapper> messageQueue) {
    this.messageQueue = messageQueue;
  }

  @Override
  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
    ChannelContext cs = null;
    ChannelContext.RegisteredChannelContext ecc = null;
    try {
      Channel channel = e.getChannel();
      TransportMessage tm = (TransportMessage) e.getMessage();
      cs = ChannelContext.getChannelContext(channel);
      ecc = cs.getRegisteredChannelContext();

      final Integer senderID = ecc.getRemoteID();
      final MessageWrapper mw = new MessageWrapper(senderID, tm);
      messageQueue.add(mw);
    } catch (NullPointerException ee) {
      ee.printStackTrace();
    }
    ctx.sendUpstream(e);
  }

}