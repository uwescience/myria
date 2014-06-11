package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.channel.Channel;

/**
 * A logical stream I/O channel.
 * */
public abstract class StreamIOChannel {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamIOChannel.class);

  /**
   * Physical channel for IO.
   * */
  private volatile Channel ioChannel;

  /**
   * ID.
   * */
  private final StreamIOChannelID id;

  /**
   * @param ecID stream IO channel ID.
   * */
  public StreamIOChannel(final StreamIOChannelID ecID) {
    id = ecID;
  }

  /**
   * @return the associated io channel.
   * */
  public final Channel getIOChannel() {
    return ioChannel;
  }

  /**
   * Attach an IO channel.
   * 
   * @param ioChannel the IO channel to associate.
   * */
  final void attachIOChannel(final Channel ioChannel) {
    this.ioChannel = ioChannel;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(this.getClass().getSimpleName() + " ID: {} attached to physical channel: {}", id, ioChannel);
    }
  }

  /**
   * Detach IO channel.
   * */
  final void detachIOChannel() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(this.getClass().getSimpleName() + " ID: {} detached from physical channel: {}", id, ioChannel);
    }
    ioChannel = null;
  }

  /**
   * @return my stream channel ID.
   * */
  public final StreamIOChannelID getID() {
    return id;
  }

}
