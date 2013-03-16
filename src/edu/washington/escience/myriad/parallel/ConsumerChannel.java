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

  public ConsumerChannel(final QuerySubTreeTask ownerTask, final Consumer consumer, final int remoteID) {
    this(ownerTask, consumer, new ExchangeChannelID(consumer.getOperatorID().getLong(), remoteID));
  }

  public ConsumerChannel(final QuerySubTreeTask ownerTask, final Consumer consumer, final ExchangeChannelID ecID) {
    id = ecID;
    this.ownerTask = ownerTask;
    op = consumer;
  }

  final Consumer op;

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
  final QuerySubTreeTask ownerTask;

  public Channel getIOChannel() {
    return ioChannel;
  }

  public void attachIOChannel(final Channel ioChannel) {
    this.ioChannel = ioChannel;
  }

  public void dettachIOChannel() {
    ioChannel = null;
  }

  public ExchangeChannelID getExchangeChannelID() {
    return id;
  }
}
