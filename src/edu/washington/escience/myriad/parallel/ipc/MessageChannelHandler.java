package edu.washington.escience.myriad.parallel.ipc;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelUpstreamHandler;

/**
 * Process messages.
 * 
 * @param <M> the message type.
 */
public interface MessageChannelHandler<M> extends ChannelUpstreamHandler {

  /**
   * process a message sent from remoteID.
   * 
   * @param ch the channel through which the message is sent.
   * @param ipcID source remote IPC ID.
   * @param message the message.
   * @return true if the message is successfully processed.If the return value is false, the caller should pause to push
   *         data into the processor and do some message caching. If the return value is false and still the data are
   *         pushed into the processor, the semantic is undefined.
   * */
  boolean processMessage(final Channel ch, final int ipcID, final M message);

}
