package edu.washington.escience.myria.parallel.ipc;

import org.jboss.netty.channel.Channel;

import edu.washington.escience.myria.util.Attachmentable;

/**
 * Short message processor.
 *
 * @param <PAYLOAD> the type of application defined data the short message processor is going to process.
 */
public interface ShortMessageProcessor<PAYLOAD> extends Attachmentable {

  /**
   * process a short message sent from the channel.
   *
   * @param ch the channel through which the message is sent.
   * @param message the message.
   * @return true if the message is successfully processed.If the return value is false, the caller should pause to push
   *         data into the processor and do some message caching. If the return value is false and still the data are
   *         pushed into the processor, the semantic is undefined.
   * */
  boolean processMessage(final Channel ch, final IPCMessage.Data<PAYLOAD> message);
}
