package edu.washington.escience.myriad.parallel.ipc;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myriad.parallel.Producer;
import edu.washington.escience.myriad.parallel.QuerySubTreeTask;

/**
 * 
 * An {@link StreamOutputChannel} represents a partition of {@link Producer}.
 * 
 * */
public class StreamOutputChannel extends StreamIOChannel {

  /** The logger for this class. */
  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(StreamOutputChannel.class.getName());

  /**
   * @param ownerTask the task who owns the output channel.
   * @param ecID exchange channel ID.
   * */
  public StreamOutputChannel(final QuerySubTreeTask ownerTask, final StreamIOChannelID ecID) {
    super(ecID);
    ownerQuerySubTreeTask = ownerTask;
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is not able to process
   * writes.
   * */
  public final void notifyOutputDisabled() {
    ownerQuerySubTreeTask.notifyOutputDisabled(getID());
  }

  /**
   * Call the method if the physical output device used by this {@link StreamOutputChannel} is able to process writes.
   * */
  public final void notifyOutputEnabled() {
    ownerQuerySubTreeTask.notifyOutputEnabled(getID());
  }

  /**
   * Physical channel for IO.
   * */
  private volatile Channel ioChannel;

  /**
   * owner task.
   * */
  private final QuerySubTreeTask ownerQuerySubTreeTask;

  @Override
  public final String toString() {
    return "StreamOutputChannel{ ID: " + getID() + ",IOChannel: " + ioChannel + " }";
  }
}
