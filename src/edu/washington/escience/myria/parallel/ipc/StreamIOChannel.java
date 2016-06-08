package edu.washington.escience.myria.parallel.ipc;

import java.util.concurrent.atomic.AtomicReference;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myria.util.concurrent.ThreadStackDump;

/**
 * A logical stream I/O channel.
 * */
public abstract class StreamIOChannel {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(StreamIOChannel.class);

  /**
   * Physical channel for IO.
   * */
  private final AtomicReference<Channel> ioChannel;

  /**
   * ID.
   * */
  private final StreamIOChannelID id;

  /**
   * @param ecID stream IO channel ID.
   * */
  public StreamIOChannel(final StreamIOChannelID ecID) {
    id = ecID;
    ioChannel = new AtomicReference<Channel>();
  }

  /**
   * @return the associated io channel.
   * */
  public final Channel getIOChannel() {
    return ioChannel.get();
  }

  /**
   * Attach an IO channel.
   *
   * @param ioChannel the IO channel to associate.
   * @return the old channel.
   * */
  final Channel attachIOChannel(final Channel ioChannel) {
    Channel oldOne = this.ioChannel.getAndSet(ioChannel);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          this.getClass().getSimpleName() + " ID: {} attached to physical channel: {}",
          id,
          ChannelContext.channelToString(ioChannel),
          new ThreadStackDump());
    }
    return oldOne;
  }

  /**
   * Detach IO channel.
   *
   * @return the old
   * */
  final Channel detachIOChannel() {
    Channel ch = ioChannel.getAndSet(null);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          this.getClass().getSimpleName() + " ID: {} detached from physical channel: {}",
          id,
          ChannelContext.channelToString(ch),
          new ThreadStackDump());
    }
    return ch;
  }

  /**
   * @return my stream channel ID.
   * */
  public final StreamIOChannelID getID() {
    return id;
  }
}
