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

  public ProducerChannel(final QuerySubTreeTask ownerTask, final Producer op, final ExchangeChannelID ecID) {
    id = ecID;
    ownerQuerySubTreeTask = ownerTask;
    this.op = op;
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

  final Producer op;

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
