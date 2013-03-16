package edu.washington.escience.myriad.parallel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.parallel.ipc.ChannelContext;
import edu.washington.escience.myriad.parallel.ipc.MessageChannelHandler;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.Builder;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

@Sharable
public class MasterDataHandler extends SimpleChannelUpstreamHandler implements MessageChannelHandler<TransportMessage> {

  /**
   * A simple message wrapper for use in current queue-based master message processing.
   * */
  public static class MessageWrapper {
    public int senderID;
    public TransportMessage message;

    public MessageWrapper(final int senderID, final TransportMessage message) {
      this.senderID = senderID;
      this.message = message;
    }
  }

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterDataHandler.class.getName());

  /**
   * messageQueue.
   * */
  private final LinkedBlockingQueue<MessageWrapper> messageQueue;

  /**
   * constructor.
   * */
  MasterDataHandler(final LinkedBlockingQueue<MessageWrapper> messageQueue) {
    this.messageQueue = messageQueue;
    channel2OperatorID = new ConcurrentHashMap<Integer, Long>();
  }

  private final ConcurrentHashMap<Integer, Long> channel2OperatorID;

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    ChannelContext cs = null;
    ChannelContext.RegisteredChannelContext ecc = null;
    final Channel channel = e.getChannel();
    cs = ChannelContext.getChannelContext(channel);
    ecc = cs.getRegisteredChannelContext();
    final Integer senderID = ecc.getRemoteID();
    TransportMessage tm = (TransportMessage) e.getMessage();
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("received a message from " + senderID + ": of type " + tm.getType());
    }
    if (tm.getType() == TransportMessageType.DATA) {
      DataMessage dm = tm.getData();
      switch (dm.getType()) {
        case NORMAL:
        case EOI:
          Builder tmB = tm.toBuilder();
          tmB.getDataBuilder().setOperatorID(channel2OperatorID.get(channel.getId()));
          tm = tmB.build();
          while (!processMessage(channel, senderID, tm)) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER
                  .error("unable to push data for processing. Normally this should not happen. Maybe the input buffer is out of memory.");
            }
          }
          break;
        case BOS:
          channel2OperatorID.put(channel.getId(), tm.getData().getOperatorID());
          break;
        case EOS:
          tmB = tm.toBuilder();
          tmB.getDataBuilder().setOperatorID(channel2OperatorID.get(channel.getId()));
          tm = tmB.build();
          channel2OperatorID.remove(channel.getId());
          while (!processMessage(channel, senderID, tm)) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER
                  .error("unable to push data for processing. Normally this should not happen. Maybe the input buffer is out of memory.");
            }
          }
          break;
      }
    } else {
      if (tm.getType() == TransportMessageType.CONTROL) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Received a control message from : " + senderID + " of type: "
              + tm.getControl().getType().name());
        }
      }
      while (!processMessage(channel, senderID, tm)) {
        if (LOGGER.isErrorEnabled()) {
          LOGGER
              .error("unable to push data for processing. Normally this should not happen. Maybe the input buffer is out of memory.");
        }
      }
    }
    ctx.sendUpstream(e);
  }

  @Override
  public boolean processMessage(final Channel channel, final int remoteID, final TransportMessage message) {
    final MessageWrapper mw = new MessageWrapper(remoteID, message);
    return messageQueue.offer(mw);
  }

}