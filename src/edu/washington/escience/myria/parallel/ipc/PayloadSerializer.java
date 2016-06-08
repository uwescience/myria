package edu.washington.escience.myria.parallel.ipc;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * Serialize and Deserialize IPC payloads.
 * */
public interface PayloadSerializer {

  /**
   * @return serialized result.
   * @param p the payload to get serialized.
   * @throws IOException if any I/O error occurs.
   * */
  ChannelBuffer serialize(Object p) throws IOException;

  /**
   * De-serialize payload.
   *
   * @param buffer serialized buffer.
   * @param processor if the channel from which the message is received is a stream input channel, the processor of the
   *          input buffer is provided. Or null, if it's not a stream message.
   * @param attachment the attachment that above application layer set. If it's a payload in a stream input channel, the
   *          attachment is the one set in {@link StreamInputBuffer#setAttachment(Object)}. Otherwise, it's the value
   *          set in {@link ShortMessageProcessor#setAttachment(Object)}.
   * @throws IOException if any I/O error occurs.
   * @return Deserialized payload object.
   * */
  Object deSerialize(ChannelBuffer buffer, Object processor, Object attachment) throws IOException;
}
