package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.channel.AbstractChannelSink;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelState;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.LoggerFactory;

/**
 * ChannelSink implementation for InJVM channels. <br>
 * All the messages reaching the sink will automatically be pushed into the associated @{link MessageChannelHandler}.
 *
 * */
public class InJVMLoopbackChannelSink extends AbstractChannelSink {

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(InJVMLoopbackChannelSink.class);

  @Override
  public final void eventSunk(final ChannelPipeline pipeline, final ChannelEvent e)
      throws Exception {
    InJVMChannel channel = (InJVMChannel) e.getChannel();
    ChannelFuture future = e.getFuture();

    try {
      if (e instanceof ChannelStateEvent) {
        ChannelStateEvent event = (ChannelStateEvent) e;
        ChannelState state = event.getState();
        Object value = event.getValue();

        switch (state) {
          case OPEN:
            if (Boolean.FALSE.equals(value)) {
              channel.closeNow(future);
            } else {
              future.setSuccess();
            }
            break;
          case BOUND:
            if (value != null) {
              Channels.fireChannelBound(channel, channel.getLocalAddress());
              future.setSuccess();
            } else {
              channel.closeNow(future);
            }
            break;
          case CONNECTED:
            if (value != null) {
              Channels.fireChannelConnected(channel, InJVMChannel.PSEUDO_SERVER_ADDRESS);
              future.setSuccess();
            } else {
              channel.closeNow(future);
            }
            break;
          case INTEREST_OPS:
            int newInterest = (Integer) event.getValue();
            if (channel.getInterestOps() != newInterest) {
              channel.setInterestOpsNow(newInterest);
              Channels.fireChannelInterestChanged(channel);
            }
            future.setSuccess();
            break;
        }
      } else if (e instanceof MessageEvent) {
        // this.messageProcessor.processMessage(channel, myIPCID, (M) ((MessageEvent) e).getMessage());
        Channels.fireMessageReceived(channel, ((MessageEvent) e).getMessage());
        future.setSuccess();
        Channels.fireWriteComplete(channel, 1);
      } else {
        if (LOGGER.isErrorEnabled()) {
          LOGGER.error("Unexpected channel event catched at sink. {}", e);
        }
      }
    } catch (Throwable t) {
      future.setFailure(t);
    }
    if (!future.isDone()) {
      future.setFailure(null);
    }
  }
}
