package edu.washington.escience.myriad.parallel;

import org.jboss.netty.channel.Channel;

/**
 * 
 * An ExchangeChannel represents a partition of a {@link Consumer}/{@link Producer} operator.
 * 
 * It's an input ExchangeChannel if it's a partition of a {@link Consumer}. Otherwise, an output ExchangeChannel.
 * 
 * */
public class ProducerChannel {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ProducerChannel.class.getName());

  /**
   * @param ownerTask the task who owns the output channel.
   * @param ecID exchange channel ID.
   * */
  public ProducerChannel(final QuerySubTreeTask ownerTask, final ExchangeChannelID ecID) {
    id = ecID;
    ownerQuerySubTreeTask = ownerTask;
    // this.op = op;
  }

  /**
   * Call the method if the physical output device used by this {@link ProducerChannel} is not able to process writes.
   * */
  public final void notifyOutputDisabled() {
    ownerQuerySubTreeTask.notifyOutputDisabled(id);
  }

  /**
   * Call the method if the physical output device used by this {@link ProducerChannel} is able to process writes.
   * */
  public final void notifyOutputEnabled() {
    ownerQuerySubTreeTask.notifyOutputEnabled(id);
  }

  // final Producer op;

  /**
   * Physical channel for IO.
   * */
  private volatile Channel ioChannel;

  /**
   * ID.
   * */
  private final ExchangeChannelID id;

  /**
   * owner task.
   * */
  private final QuerySubTreeTask ownerQuerySubTreeTask;

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
  public final void attachIOChannel(final Channel ioChannel) {
    this.ioChannel = ioChannel;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("ProducerChannel ID: {} attached to output channel: IOChannel: {}, ", id, ioChannel);
    }
  }

  /**
   * Detach IO channel.
   * */
  public final void dettachIOChannel() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("ProducerChannel ID: {} detatch from output channel: IOChannel: {}, ", id, ioChannel);
    }
    ioChannel = null;
  }

  /**
   * @return my logical exchange channel ID.
   * */
  public final ExchangeChannelID getExchangeChannelID() {
    return id;
  }

  @Override
  public final String toString() {
    return "ProducerChannel{ ID: " + id + ",IOChannel: " + ioChannel + " }";
  }
}
