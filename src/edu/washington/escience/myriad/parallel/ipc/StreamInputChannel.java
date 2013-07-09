package edu.washington.escience.myriad.parallel.ipc;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myriad.parallel.Consumer;
import edu.washington.escience.myriad.parallel.QuerySubTreeTask;

/**
 * 
 * An {@link StreamInputChannel} represents a partition of a {@link Consumer} input .
 * 
 * */
public class StreamInputChannel extends StreamIOChannel {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamInputChannel.class.getName());

  /**
   * @param ownerTask the task who owns the output channel.
   * @param consumer the owner consumer operator.
   * @param remoteID from which worker the input data comes.
   * */
  public StreamInputChannel(final QuerySubTreeTask ownerTask, final Consumer consumer, final int remoteID) {
    this(ownerTask, consumer, new StreamIOChannelID(consumer.getOperatorID().getLong(), remoteID));
  }

  /**
   * @param ownerTask the task who owns the output channel.
   * @param consumer the owner consumer operator.
   * @param ecID ID.
   * */
  public StreamInputChannel(final QuerySubTreeTask ownerTask, final Consumer consumer, final StreamIOChannelID ecID) {
    super(ecID);
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

  @Override
  public final String toString() {
    return "ConsumerChannel{ ID: " + getID() + ",Op: " + op + ",IOChannel: " + ioChannel + " }";
  }
}
