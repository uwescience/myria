package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.channel.Channel;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.operator.network.Consumer;
import edu.washington.escience.myria.operator.network.Producer;
import edu.washington.escience.myria.util.concurrent.ReentrantSpinLock;

/**
 * The data structure recording the logical role of {@link StreamInputChannel} and {@link StreamOutputChannel} that an
 * IO channel plays.
 * <p>
 * An IO channel can be an input of a {@link Consumer} operator (inputChannel) and in the same time an output of a
 * {@link Producer} operator (outputChannel).
 * */
class StreamIOChannelPair {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamIOChannelPair.class);

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
    Channel oldChannel = null;
    inputMappingLock.lock();
    try {
      if (inputStreamChannel != null) {
        oldChannel = inputStreamChannel.detachIOChannel();
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(String.format("Stream input channel %1$s disassociated from physical channel %2$s.",
              inputStreamChannel, oldChannel));
        }
      }
      inputStreamChannel = inputChannel;
      inputChannel.attachIOChannel(ioChannel);

    } finally {
      inputMappingLock.unlock();
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(String.format("Stream input channel %1$s associates to physical channel %2$s.", inputChannel,
          ioChannel));
    }
    if (oldChannel != null) {
      ChannelContext.resumeRead(oldChannel).awaitUninterruptibly();
    }
  }

  /**
   * Remove the link between a logical input channel and a physical IO channel. And the IO channel reading gets resumed
   * anyway.
   * */
  final void deMapInputChannel() {
    Channel channel = null;
    StreamInputChannel<?> old = null;

    inputMappingLock.lock();
    try {
      if (inputStreamChannel != null) {
        old = inputStreamChannel;
        channel = inputStreamChannel.detachIOChannel();
        inputStreamChannel = null;
      }
    } finally {
      inputMappingLock.unlock();
    }
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(String.format("Stream input channel %1$s disassociated from physical channel %2$s.", old, channel));
    }
    if (channel != null) {
      ChannelContext.resumeRead(channel).awaitUninterruptibly();
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
        outputStreamChannel.detachIOChannel();
        outputStreamChannel = null;
      }
    } finally {
      outputMappingLock.unlock();
    }
  }

}
