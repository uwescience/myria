package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelSink;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.slf4j.LoggerFactory;

/**
 * This class channel is used where no meaningful channel instances can be used. Most probability it happens when a
 * channel is null but a future must be generated.
 * */
public final class NullChannel extends InJVMChannel {

  /** The logger for this class. */
  protected static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(NullChannel.class);

  /**
   * Singleton instance.
   */
  public static final NullChannel NULL;
  /**
   * A do nothing pipeline.
   */
  private static final ChannelPipeline PIPELINE;

  static {
    PIPELINE =
        new DefaultChannelPipeline() {

          @Override
          public void sendUpstream(final ChannelEvent e) {}

          @Override
          public void sendDownstream(final ChannelEvent e) {}

          @Override
          public void attach(final Channel channel, final ChannelSink sink) {}

          @Override
          public boolean isAttached() {
            return true;
          }
        };
    NULL = new NullChannel();
  }

  /**
   * Constructor.
   */
  private NullChannel() {
    super(PIPELINE, null);
    Channels.fireChannelClosed(this);
    setClosed();
  }
}
