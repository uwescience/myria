package edu.washington.escience.myriad.parallel;

import org.jboss.netty.channel.Channel;

/**
 * 
 * An ExchangeChannel represents a partition of a {@link Consumer}/{@link Producer} operator.
 * 
 * It's an input ExchangeChannel if it's a partition of a {@link Consumer}. Otherwise, an output ExchangeChannel.
 * 
 * */
public class ConsumerChannel {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ConsumerChannel.class.getName());

  /**
   * @param ownerTask the task who owns the output channel.
   * @param consumer the owner consumer operator.
   * @param remoteID from which worker the input data comes.
   * */
  public ConsumerChannel(final QuerySubTreeTask ownerTask, final Consumer consumer, final int remoteID) {
    this(ownerTask, consumer, new ExchangeChannelID(consumer.getOperatorID().getLong(), remoteID));
  }

  /**
   * @param ownerTask the task who owns the output channel.
   * @param consumer the owner consumer operator.
   * @param ecID exchange channel ID.
   * */
  public ConsumerChannel(final QuerySubTreeTask ownerTask, final Consumer consumer, final ExchangeChannelID ecID) {
    id = ecID;
    this.ownerTask = ownerTask;
    op = consumer;
  }

  /**
   * the operator to whom this channel serves as an input.
   * */
  private final Consumer op;

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
  private final QuerySubTreeTask ownerTask;

  /**
   * @return the owner task
   * */
  public final QuerySubTreeTask getOwnerTask() {
    return ownerTask;
  }

  /**
   * @return the owner task
   * */
  public final Consumer getOwnerConsumer() {
    return op;
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
  public final void attachIOChannel(final Channel ioChannel) {
    this.ioChannel = ioChannel;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("ConsumerChannel ID: {} attached to output channel: IOChannel: {}, ", id, ioChannel);
    }
  }

  /**
   * Detach IO channel.
   * */
  public final void dettachIOChannel() {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("ConsumerChannel ID: {} detatch from output channel: IOChannel: {}, ", id, ioChannel);
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
    return "ConsumerChannel{ ID: " + id + ",Op: " + op + ",IOChannel: " + ioChannel + " }";
  }
}
