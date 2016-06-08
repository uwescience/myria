package edu.washington.escience.myria.parallel.ipc;

import java.util.Queue;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myria.util.AttachmentableAdapter;

/**
 * The simplest implementation of {@link ShortMessageProcessor}. The messages are simply appended into a FIFO queue.
 *
 * @param <PAYLOAD> the type of application defined data the short message processor is going to process.
 * */
public class QueueBasedShortMessageProcessor<PAYLOAD> extends AttachmentableAdapter
    implements ShortMessageProcessor<PAYLOAD> {

  /**
   * Logger.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(QueueBasedShortMessageProcessor.class);

  /**
   * The queue for message storage.
   * */
  private final Queue<IPCMessage.Data<PAYLOAD>> messageQueue;

  /**
   * @param messageQueue the storage queue.
   * */
  public QueueBasedShortMessageProcessor(final Queue<IPCMessage.Data<PAYLOAD>> messageQueue) {
    this.messageQueue = messageQueue;
  }

  @Override
  public final boolean processMessage(final Channel channel, final IPCMessage.Data<PAYLOAD> m) {
    return messageQueue.offer(m);
  }
}
