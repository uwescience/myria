package edu.washington.escience.myriad.parallel.ipc;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.Producer;
import edu.washington.escience.myriad.util.IPCUtils;
import edu.washington.escience.myriad.util.ReentrantSpinLock;

/**
 * The data structure recording the logical role of {@link StreamInputChannel} and {@link StreamOutputChannel} that an
 * IO channel plays.
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
  private final ReentrantSpinLock inputMappingLock = new ReentrantSpinLock();

  /**
   * The lock protecting the consistency of output flow control setup.
   * */
  private final ReentrantSpinLock outputMappingLock = new ReentrantSpinLock();

  /**
   * The input stream channel.
   * */
  private StreamInputChannel<?> inputStreamChannel;

  /**
   * The output stream channel.
   * */
  private StreamOutputChannel<?> outputStreamChannel;

  /**
   * Owner channel context. A StreamIOChannelPair must be attached to a channel.
   * */
  private final ChannelContext ownerChannelContext;

  /**
   * @param ownerCTX owner ChannelContext.
   * */
  StreamIOChannelPair(final ChannelContext ownerCTX) {
    ownerChannelContext = Preconditions.checkNotNull(ownerCTX);
  }

  /**
   * @return the input channel in the pair.
   * @param <PAYLOAD> the payload type.
   * */
  @SuppressWarnings("unchecked")
  final <PAYLOAD> StreamInputChannel<PAYLOAD> getInputChannel() {
    inputMappingLock.lock();
    try {
      return (StreamInputChannel<PAYLOAD>) inputStreamChannel;
    } finally {
      inputMappingLock.unlock();
    }
  }

  /**
   * @return the output channel in the pair.
   * @param <PAYLOAD> the payload type.
   * */
  @SuppressWarnings("unchecked")
  final <PAYLOAD> StreamOutputChannel<PAYLOAD> getOutputChannel() {
    outputMappingLock.lock();
    try {
      return (StreamOutputChannel<PAYLOAD>) outputStreamChannel;
    } finally {
      outputMappingLock.unlock();
    }
  }

  /**
   * Link the logical inputChannel with the physical ioChannel.
   * 
   * @param inputChannel the logical channel.
   * */
  final void mapInputChannel(final StreamInputChannel<?> inputChannel) {
    Preconditions.checkNotNull(inputChannel);
    Channel ioChannel = ownerChannelContext.getChannel();
    inputMappingLock.lock();
    try {
      if (inputStreamChannel != null) {
        deMapInputChannel();
      }
      inputStreamChannel = inputChannel;
      inputChannel.attachIOChannel(ioChannel);
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace(String.format("Stream input channel %1$s associates to physical channel %2$s.",
            inputStreamChannel, ioChannel));
      }
    } finally {
      inputMappingLock.unlock();
    }
  }

  /**
   * Remove the link between a logical input channel and a physical IO channel. And the IO channel reading gets resumed
   * anyway.
   * */
  final void deMapInputChannel() {
    inputMappingLock.lock();
    try {
      if (inputStreamChannel != null) {
        Channel channel = inputStreamChannel.getIOChannel();
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(String.format("Stream input channel %1$s disassociated from physical channel %2$s.",
              inputStreamChannel, channel));
        }
        inputStreamChannel.dettachIOChannel();
        inputStreamChannel = null;
        if (channel != null) {
          IPCUtils.resumeRead(channel).awaitUninterruptibly();
        }
      }
    } finally {
      inputMappingLock.unlock();
    }
  }

  /**
   * Link the logical outputChannel with the physical ioChannel.
   * 
   * @param outputChannel the logical channel.
   * */
  final void mapOutputChannel(final StreamOutputChannel<?> outputChannel) {
    Preconditions.checkNotNull(outputChannel);
    Channel ioChannel = ownerChannelContext.getChannel();
    outputMappingLock.lock();
    try {
      if (outputStreamChannel != null) {
        deMapOutputChannel();
      }
      outputStreamChannel = outputChannel;
      outputChannel.attachIOChannel(ioChannel);
    } finally {
      outputMappingLock.unlock();
    }
  }

  /**
   * Remove the link between a logical output channel and a physical IO channel.
   * */
  final void deMapOutputChannel() {
    outputMappingLock.lock();
    try {
      if (outputStreamChannel != null) {
        outputStreamChannel.dettachIOChannel();
        outputStreamChannel = null;
      }
    } finally {
      outputMappingLock.unlock();
    }
  }

  /**
   * Resume reading from the input channel.
   * 
   * @return the future of this action, null if no physical IO channel is associated.
   * */
  public final ChannelFuture resumeRead() {
    inputMappingLock.lock();
    try {
      if (inputStreamChannel != null) {
        Channel ch = inputStreamChannel.getIOChannel();
        if (ch != null && !ch.isReadable()) {
          return IPCUtils.resumeRead(ch);
        }
      }
    } finally {
      inputMappingLock.unlock();
    }
    return null;
  }

  /**
   * Pause reading from the input channel.
   * 
   * @return the future of this action, null if no physical IO channel is associated.
   * */
  public final ChannelFuture pauseRead() {
    inputMappingLock.lock();
    try {
      if (inputStreamChannel != null) {
        Channel ch = inputStreamChannel.getIOChannel();
        if (ch != null && ch.isReadable()) {
          return IPCUtils.pauseRead(ch);
        }
      }
    } finally {
      inputMappingLock.unlock();
    }
    return null;
  }

}
