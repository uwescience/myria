package edu.washington.escience.myriad.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.parallel.ipc.ChannelContext;
import edu.washington.escience.myriad.parallel.ipc.MessageChannelHandler;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

public class QueueBasedMessageHandler implements ChannelUpstreamHandler, MessageChannelHandler<TransportMessage> {

  public static class TestMessageWrapper {
    public int senderID;

    public TransportMessage message;

    public TestMessageWrapper(final int senderID, final TransportMessage message) {
      this.senderID = senderID;
      this.message = message;
    }
  }

  static Logger logger = LoggerFactory.getLogger(QueueBasedMessageHandler.class);
  final Queue<TestMessageWrapper> messageQueue;

  public QueueBasedMessageHandler(Queue<TestMessageWrapper> messageQueue) {
    this.messageQueue = messageQueue;
    channel2OperatorID = new ConcurrentHashMap<Channel, Long>();
  }

  private final ConcurrentHashMap<Channel, Long> channel2OperatorID;

  @Override
  public boolean processMessage(Channel channel, int remoteID, TransportMessage tm) {
    if (tm.getType() == TransportMessageType.DATA) {

      DataMessage dm = tm.getData();
      switch (dm.getType()) {
        case NORMAL:
        case EOI:
          Long operatorID = channel2OperatorID.get(channel);
          if (operatorID == null) {
            logger.error("Data message received with null operatorID.");
            return true;
          }
          TransportMessage.Builder tmB = tm.toBuilder();
          tmB.getDataBuilder().setOperatorID(operatorID);
          tm = tmB.build();
          break;
        case BOS:
          channel2OperatorID.put(channel, tm.getData().getOperatorID());
          break;
        case EOS:
          operatorID = channel2OperatorID.get(channel);
          if (operatorID == null) {
            logger.error("Data message received with null operatorID.");
            return true;
          }
          channel2OperatorID.remove(channel);
          break;
      }
    }
    return messageQueue.offer(new TestMessageWrapper(remoteID, tm));
  }

  @Override
  public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
    if (e instanceof MessageEvent) {
      final Channel channel = e.getChannel();
      final ChannelContext cs = (ChannelContext) channel.getAttachment();
      final ChannelContext.RegisteredChannelContext ecc = cs.getRegisteredChannelContext();
      final Integer senderID = ecc.getRemoteID();
      MessageEvent m = MessageEvent.class.cast(e);
      TransportMessage tm = TransportMessage.class.cast(m.getMessage());
      while (!processMessage(channel, senderID, tm)) {
        if (logger.isErrorEnabled()) {
          logger
              .error("unable to push data for processing. Normally this should not happen. Maybe the input buffer is out of memory.");
        }
      }
    }
    ctx.sendUpstream(e);
  }
}