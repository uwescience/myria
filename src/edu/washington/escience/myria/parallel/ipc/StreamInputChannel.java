package edu.washington.escience.myria.parallel.ipc;

import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.FailedChannelFuture;
import org.jboss.netty.channel.SucceededChannelFuture;

import edu.washington.escience.myria.operator.network.Consumer;

/**
 *
 * An {@link StreamInputChannel} represents a partition of a {@link Consumer} input .
 *
 * @param <PAYLOAD> the type of payload that this input channel will receive.
 * */
public class StreamInputChannel<PAYLOAD> extends StreamIOChannel {

  /** The logger for this class. */
  static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(StreamInputChannel.class);

  /**
   * The input buffer into which the messages from this channel should be pushed.
   * */
  private final StreamInputBuffer<PAYLOAD> inputBuffer;

  /**
   * If this logical input channel is paused.
   */
  private final AtomicBoolean paused = new AtomicBoolean(false);

  /**
   * release this logical input channel.
   */
  public final void release() {
    Channel channel = detachIOChannel();
    if (channel != null && channel.isConnected()) {
      ChannelContext.resumeRead(channel);
    }
  }

  /**
   * @param ib the destination input buffer.
   * @param ecID exchange channel ID.
   * */
  StreamInputChannel(final StreamIOChannelID ecID, final StreamInputBuffer<PAYLOAD> ib) {
    super(ecID);
    inputBuffer = ib;
  }

  @Override
  public final String toString() {
    return "StreamInput{ ID: "
        + getID()
        + ", IOChannel: "
        + ChannelContext.channelToString(getIOChannel())
        + " }";
  }

  /**
   * @return the destination input buffer.
   * */
  final StreamInputBuffer<PAYLOAD> getInputBuffer() {
    return inputBuffer;
  }

  /**
   * pause the read from this logical input channel, no matter the state of the underlying physical input channel.
   *
   * @return future of this operation.
   */
  public ChannelFuture pauseRead() {
    if (this.paused.compareAndSet(false, true)) {
      Channel ch = getIOChannel();
      if (ch != null) {
        return ChannelContext.pauseRead(ch);
      }
    }

    // if there is no physical channel attached, or it's already paused.
    return new SucceededChannelFuture(NullChannel.NULL);
  }

  /**
   * Resume the read of all IO channels that are inputs of this input buffer.
   *
   * @return ChannelGroupFuture denotes the future of the resume read action.
   * */
  public ChannelFuture resumeRead() {
    if (this.paused.compareAndSet(true, false)) {
      Channel ch = getIOChannel();
      if (ch != null) {
        return ChannelContext.resumeRead(ch);
      }
      return new FailedChannelFuture(NullChannel.NULL, new NullPointerException());
    }
    return new SucceededChannelFuture(NullChannel.NULL);
  }
}
