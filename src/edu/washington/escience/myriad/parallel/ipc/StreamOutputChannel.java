package edu.washington.escience.myriad.parallel.ipc;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myriad.parallel.Producer;
import edu.washington.escience.myriad.parallel.QuerySubTreeTask;

/**
 * 
 * An {@link StreamOutputChannel} represents a partition of {@link Producer}.
 * 
 * */
public class StreamOutputChannel {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamOutputChannel.class.getName());

  /**
   * @param ownerTask the task who owns the output channel.
   * @param ecID exchange channel ID.
   * */
  public StreamOutputChannel(final QuerySubTreeTask ownerTask, final StreamIOChannelID ecID) {
    id = ecID;
    ownerQuerySubTreeTask = ownerTask;
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is not able to process
   * writes.
   * */
  public final void notifyOutputDisabled() {
    ownerQuerySubTreeTask.notifyOutputDisabled(id);
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is able to process writes.
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
  private final StreamIOChannelID id;

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
      LOGGER.debug("StreamOutputChannel ID: {} detatch from output channel: IOChannel: {}, ", id, ioChannel);
    }
    ioChannel = null;
  }

  /**
   * @return my logical exchange channel ID.
   * */
  public final StreamIOChannelID getID() {
    return id;
  }

  @Override
  public final String toString() {
    return "StreamOutputChannel{ ID: " + id + ",IOChannel: " + ioChannel + " }";
  }
}
