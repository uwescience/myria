package edu.washington.escience.myria.parallel;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedInputStream;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.parallel.ipc.PayloadSerializer;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * This class monitors all the input/output IPC data. It makes sure that all input data are of {@link TransportMessage}
 * type. And it does all IPC exception catching and recording.
 * */
@Sharable
public class TransportMessageSerializer implements PayloadSerializer {

  /** The logger for this class. */
  protected static final Logger LOGGER = LoggerFactory.getLogger(TransportMessageSerializer.class);

  @Override
  public final ChannelBuffer serialize(final Object m) {
    Preconditions.checkNotNull(m);
    // m has only 3 possibilities:
    if (m instanceof TransportMessage) {
      // case 1: TransportMessage.QUERY
      // case 2: TransportMessage.CONTROL
      // TransportMessage.DATA is not possible to occur here
      return ChannelBuffers.wrappedBuffer(((TransportMessage) m).toByteArray());
    } else if (m instanceof TupleBatch) {
      // case 3: TupleBatch
      TupleBatch tb = (TupleBatch) m;
      if (!tb.isEOI()) {
        return ChannelBuffers.wrappedBuffer(((TupleBatch) m).toTransportMessage().toByteArray());
      } else {
        return ChannelBuffers.wrappedBuffer(IPCUtils.EOI.toByteArray());
      }
    } else {
      throw new IllegalArgumentException(
          MyriaConstants.SYSTEM_NAME
              + " IPC only supports "
              + TransportMessage.class.getSimpleName()
              + " and "
              + TupleBatch.class.getSimpleName()
              + ", but received a message of type: "
              + m.getClass().getCanonicalName());
    }
  }

  /**
   * @param buf input data buffer.
   * @return Deserialized transport message
   * @throws IOException if any IO errors.
   */
  private TransportMessage deSerializeTransportMessage(final ChannelBuffer buf) throws IOException {

    final byte[] array;
    final int offset;
    final int length = buf.readableBytes();

    if (buf.hasArray()) {
      array = buf.array();
      offset = buf.arrayOffset() + buf.readerIndex();
    } else {
      array = new byte[length];
      buf.getBytes(buf.readerIndex(), array, 0, length);
      offset = 0;
    }
    CodedInputStream cis = CodedInputStream.newInstance(array, offset, length);
    return TransportMessage.parseFrom(cis);
  }

  @Override
  public final Object deSerialize(
      final ChannelBuffer buffer, final Object processor, final Object att) throws IOException {
    TransportMessage tm = deSerializeTransportMessage(buffer);
    return tm;
  }
}
