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
  }

  /**
   * Detach IO channel.
   * */
  public final void dettachIOChannel() {
    ioChannel = null;
  }

  /**
   * @return my logical exchange channel ID.
   * */
  public final ExchangeChannelID getExchangeChannelID() {
    return id;
  }
}
