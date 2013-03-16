package edu.washington.escience.myriad.parallel;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroupFuture;

import edu.washington.escience.myriad.parallel.ipc.ChannelContext;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage.TransportMessageType;

/**
 * Flow control for both input and output. This handler should be placed at the downstream of message processors.
 * */
@Sharable
public final class FlowControlHandler extends SimpleChannelHandler {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(FlowControlHandler.class.getName());

  private final ConcurrentHashMap<ExchangeChannelID, ProducerChannel> producerChannelMap;
  private final ConcurrentHashMap<ExchangeChannelID, ConsumerChannel> consumerChannelMap;

  public FlowControlHandler(final ConcurrentHashMap<ExchangeChannelID, ConsumerChannel> consumerChannelMap,
      final ConcurrentHashMap<ExchangeChannelID, ProducerChannel> producerChannelMap) {
    this.consumerChannelMap = consumerChannelMap;
    this.producerChannelMap = producerChannelMap;
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
    final TransportMessage tm = (TransportMessage) e.getMessage();
    final Channel channel = e.getChannel();
    final ChannelContext cc = (ChannelContext) channel.getAttachment();
    final ChannelContext.RegisteredChannelContext ecc = cc.getRegisteredChannelContext();
    final Integer senderID = ecc.getRemoteID();
    ExchangeChannelPair ecp = (ExchangeChannelPair) cc.getAttachment();
    boolean isEOS = false;
    switch (tm.getType()) {
      case DATA:
        final DataMessage data = tm.getData();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("TupleBatch received from " + senderID + " to Operator: " + data.getOperatorID());
        }

        switch (data.getType()) {
          case NORMAL:
          case EOI:
            break;
          case BOS:
            // At the beginning of a stream, record the operator id.
            final long operatorID = data.getOperatorID();
            if (ecp == null) {
              ecp = new ExchangeChannelPair();
              if (!cc.setAttachmentIfAbsent(ecp)) {
                ecp = (ExchangeChannelPair) cc.getAttachment();
              }
            }
            ExchangeChannelID ecID = new ExchangeChannelID(operatorID, senderID);
            ConsumerChannel ccc = consumerChannelMap.get(ecID);
            ecp.mapInputChannel(ccc, channel);
            break;
          case EOS:
            isEOS = true;
            break;
        }
        break;
      case QUERY:
        break;
      case CONTROL:
        break;
    }
    ctx.sendUpstream(e);
    if (isEOS) {
      ecp.deMapInputChannel();
    }
  }

  /**
   * Resume the read of all IO channels that are inputs of the @{link Consumer} operator with ID of operatorID.
   * 
   * Called by query executor threads after they pull data from a previous-full InputBuffer.
   * 
   * @param consumerOp the owner @{link Consumer} operator.
   * @return ChannelGroupFuture denotes the future of the resume read action.
   * */
  public ChannelGroupFuture resumeRead(final Consumer consumerOp) {

    ConsumerChannel[] ec = consumerOp.exchangeChannels;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Resume read for operator {}, IO Channels are {", consumerOp.getOperatorID());
      for (ConsumerChannel e : ec) {
        Channel ch = e.getIOChannel();
        LOGGER.debug("{}", ch);
      }
      LOGGER.debug("}");
    }

    LinkedList<ChannelFuture> allResumeFutures = new LinkedList<ChannelFuture>();
    ChannelGroup cg = new DefaultChannelGroup();
    for (ConsumerChannel e : ec) {

      Channel ch = e.getIOChannel();
      if (ch != null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Resume read for channel {}", ch);
        }
        ChannelContext cc = ((ChannelContext) (ch.getAttachment()));
        ExchangeChannelPair ecp = (ExchangeChannelPair) cc.getAttachment();
        if (ecp != null) {
          ChannelFuture resumeFuture = ecp.resumeRead();
          if (resumeFuture != null) {
            cg.add(ch);
            allResumeFutures.add(resumeFuture);
          }
        }
      }
    }

    ChannelGroupFuture cgf = new DefaultChannelGroupFuture(cg, allResumeFutures);

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Finish resume for Operator {}", consumerOp.getOperatorID());
    }
    return cgf;
  }

  /**
   * 
   * Pause read of all IO channels which are inputs of the @{link Consumer} operator with ID operatorID.
   * 
   * Called by Netty Upstream IO worker threads after pushing a data into an InputBuffer which has only a single empty
   * slot or already full.
   * 
   * @param consumerOp @{link Consumer} operator.
   * @return ChannelGroupFuture denotes the future of the pause read action.
   * */
  public ChannelGroupFuture pauseRead(final Consumer consumerOp) {
    ConsumerChannel[] consumerChannels = consumerOp.exchangeChannels;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Pause read for operator {}, IO Channels are {", consumerOp.getOperatorID());
      for (ConsumerChannel ec : consumerChannels) {
        // here ch may be null, it means an EOS message is already received, the IO Channel is already detached from
        // this ConsumerChannel. No flow control is needed. Just ignore it.
        Channel ch = ec.getIOChannel();
        LOGGER.debug("{}", ch);
      }
      LOGGER.debug("}");
    }

    LinkedList<ChannelFuture> allPauseFutures = new LinkedList<ChannelFuture>();
    ChannelGroup cg = new DefaultChannelGroup();
    for (ConsumerChannel ec : consumerChannels) {
      Channel ch = ec.getIOChannel();
      if (ch != null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Pause read for channel {}", ch);
        }
        ChannelContext cc = ((ChannelContext) (ch.getAttachment()));
        ExchangeChannelPair ecp = (ExchangeChannelPair) cc.getAttachment();
        if (ecp != null) {
          ChannelFuture pauseFuture = ecp.pauseRead();
          if (pauseFuture != null) {
            cg.add(ch);
            allPauseFutures.add(pauseFuture);
          }
        }
      }
    }
    return new DefaultChannelGroupFuture(cg, allPauseFutures);
  }

  /**
   * Setup output flow control context.
   * 
   * {@inheritDoc}
   * */
  @Override
  public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
    Channel ioChannel = ctx.getChannel();
    ChannelContext cc = ((ChannelContext) (ioChannel.getAttachment()));
    TransportMessage tm = (TransportMessage) (e.getMessage());
    e.getFuture().addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(final ChannelFuture future) throws Exception {
        if (!future.isSuccess()) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Write failed! Cause is:" + future.getCause() + " Message is:" + e.getMessage());
          }
        }
      }
    });
    boolean isEOS = false;
    if (tm.getType() == TransportMessageType.DATA) {
      DataMessage dm = tm.getData();
      int remoteID = cc.getRegisteredChannelContext().getRemoteID();
      ExchangeChannelPair ecp = (ExchangeChannelPair) cc.getAttachment();
      switch (dm.getType()) {
        case NORMAL:
        case EOI:
          break;
        case BOS:
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("New data connection, setup flow control context.");
          }
          long operatorID = dm.getOperatorID();
          ExchangeChannelID ecID = new ExchangeChannelID(operatorID, remoteID);

          if (ecp == null) {
            ecp = new ExchangeChannelPair();
            if (!cc.setAttachmentIfAbsent(ecp)) {
              ecp = (ExchangeChannelPair) cc.getAttachment();
            }
          }
          ecp.mapOutputChannel(producerChannelMap.get(ecID), ioChannel);
          break;
        case EOS:
          isEOS = true;
          break;
      }
      if (!ioChannel.isWritable()) {// this io channel is already full of write requests
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Input buffer full for Channel {}, to remote {}", ioChannel, remoteID);
        }
        ecp.getOutputChannel().notifyOutputDisabled();
      }
    }

    ctx.sendDownstream(e);

    if (isEOS) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("EOS, remove flow control context.");
      }
      ExchangeChannelPair ecp = (ExchangeChannelPair) cc.getAttachment();
      ecp.deMapOutputChannel();
    }
  }

  /**
   * 
   * {@inheritDoc}
   */
  @Override
  public void channelInterestChanged(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
    Channel ioChannel = ctx.getChannel();
    if (ioChannel.isWritable()) {
      ChannelContext cc = ((ChannelContext) (ioChannel.getAttachment()));
      ExchangeChannelPair p = (ExchangeChannelPair) (cc.getAttachment());
      if (p != null) {
        ProducerChannel producerChannel = p.getOutputChannel();
        if (producerChannel != null) {
          int remoteID = cc.getRegisteredChannelContext().getRemoteID();
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Input buffer recovered for Channel {}, to remote {}", ioChannel, remoteID);
          }
          producerChannel.notifyOutputEnabled();
        }
      }
    }
    if (!ioChannel.isReadable()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Channel {} is changed to be unreadable.", ioChannel);
      }
    }
    ctx.sendUpstream(e);
  }

}
