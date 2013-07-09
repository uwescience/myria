package edu.washington.escience.myriad.parallel.ipc;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.Producer;
import edu.washington.escience.myriad.util.IPCUtils;

/**
 * The data structure recording the logical role of {@link StreamInputChannel} and {@link StreamOutputChannel} that an IO
 * channel plays.
 * <p>
 * An IO channel can be an input of a {@link Consumer} operator (inputChannel) and in the same time an output of a
 * {@link Producer} operator (outputChannel).
 * */
public class StreamIOChannelPair {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamIOChannelPair.class.getName());

  /**
   * The lock protecting the consistency of input flow control setup.
   * */
  private final Object inputFlowControlLock = new Object();

  /**
   * The lock protecting the consistency of output flow control setup.
   * */
  private final Object outputFlowControlLock = new Object();

  /**
   * The logical input channel.
   * */
  private StreamInputChannel inputChannel;

  /**
   * The logical output channel.
   * */
  private StreamOutputChannel outputChannel;

  /**
   * @return the input channel in the pair.
   * */
  public final StreamInputChannel getInputChannel() {
    synchronized (inputFlowControlLock) {
      return inputChannel;
    }
  }

  /**
   * @return the output channel in the pair.
   * */
  public final StreamOutputChannel getOutputChannel() {
    synchronized (outputFlowControlLock) {
      return outputChannel;
    }
  }

  /**
   * Link the logical inputChannel with the physical ioChannel.
   * 
   * @param inputChannel the logical channel.
   * @param ioChannel the physical channel.
   * */
  public final void mapInputChannel(final StreamInputChannel inputChannel, final Channel ioChannel) {
    Preconditions.checkNotNull(inputChannel);
    synchronized (inputFlowControlLock) {
      if (this.inputChannel != null) {
        deMapInputChannel();
      }
      this.inputChannel = inputChannel;
      inputChannel.attachIOChannel(ioChannel);
    }
  }

  /**
   * Remove the link between a logical input channel and a physical IO channel. And the IO channel reading gets resumed
   * anyway.
   * */
  public final void deMapInputChannel() {
    synchronized (inputFlowControlLock) {
      if (inputChannel != null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("deMap " + inputChannel.getID() + ", ioChannel: " + inputChannel.getIOChannel());
        }
        Channel channel = inputChannel.getIOChannel();
        inputChannel.dettachIOChannel();
        inputChannel = null;
        if (channel != null) {
          IPCUtils.resumeRead(channel).awaitUninterruptibly();
        }
      }
    }
  }

  /**
   * Link the logical outputChannel with the physical ioChannel.
   * 
   * @param outputChannel the logical channel.
   * @param ioChannel the physical channel.
   * */
  public final void mapOutputChannel(final StreamOutputChannel outputChannel, final Channel ioChannel) {
    Preconditions.checkNotNull(outputChannel);
    synchronized (outputFlowControlLock) {
      if (this.outputChannel != null) {
        deMapOutputChannel();
      }
      this.outputChannel = outputChannel;
      outputChannel.attachIOChannel(ioChannel);
    }
  }

  /**
   * Remove the link between a logical output channel and a physical IO channel.
   * */
  public final void deMapOutputChannel() {
    synchronized (outputFlowControlLock) {
      if (outputChannel != null) {
        outputChannel.dettachIOChannel();
        outputChannel = null;
      }
    }
  }

  /**
   * Resume reading from the input channel.
   * 
   * @return the future of this action, null if no physical IO channel is associated.
   * */
  public final ChannelFuture resumeRead() {
    synchronized (inputFlowControlLock) {
      if (inputChannel != null) {
        Channel ch = inputChannel.getIOChannel();
        if (ch != null && !ch.isReadable()) {
          return IPCUtils.resumeRead(ch);
        }
      }
    }
    return null;
  }

  /**
   * Pause reading from the input channel.
   * 
   * @return the future of this action, null if no physical IO channel is associated.
   * */
  public final ChannelFuture pauseRead() {
    synchronized (inputFlowControlLock) {
      if (inputChannel != null) {
        Channel ch = inputChannel.getIOChannel();
        if (ch != null && ch.isReadable()) {
          return IPCUtils.pauseRead(ch);
        }
      }
    }
    return null;
  }

}
