package edu.washington.escience.myriad.parallel;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedInputStream;

import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.parallel.ipc.PayloadSerializer;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.DataMessage;
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;

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
    if (m instanceof TransportMessage) {
      return ChannelBuffers.wrappedBuffer(((TransportMessage) m).toByteArray());
    } else if (m instanceof TupleBatch) {
      TupleBatch tb = (TupleBatch) m;
      if (!tb.isEOI()) {
        return ChannelBuffers.wrappedBuffer(((TupleBatch) m).toTransportMessage().toByteArray());
      } else {
        return ChannelBuffers.wrappedBuffer(IPCUtils.EOI.toByteArray());
      }
    } else {
      throw new IllegalArgumentException(MyriaConstants.SYSTEM_NAME + " IPC only supports "
          + TransportMessage.class.getSimpleName() + " and " + TupleBatch.class.getSimpleName()
          + ", but received a message of type: " + m.getClass().getCanonicalName());
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
  public final Object deSerialize(final ChannelBuffer buffer, final Object processor, final Object channelAttachment)
      throws IOException {
    TransportMessage tm = deSerializeTransportMessage(buffer);

    switch (tm.getType()) {
      case DATA:
        DataMessage dm = tm.getDataMessage();
        Schema msgOwnerOpSchema = (Schema) channelAttachment;
        switch (dm.getType()) {
          case NORMAL:
            final List<ColumnMessage> columnMessages = dm.getColumnsList();
            final Column<?>[] columnArray = new Column<?>[columnMessages.size()];
            int idx = 0;
            for (final ColumnMessage cm : columnMessages) {
              columnArray[idx++] = ColumnFactory.columnFromColumnMessage(cm, dm.getNumTuples());
            }
            final List<Column<?>> columns = Arrays.asList(columnArray);
            return new TupleBatch(msgOwnerOpSchema, columns, dm.getNumTuples());
          case EOI:
            return TupleBatch.eoiTupleBatch(msgOwnerOpSchema);
          default:
            throw new IllegalArgumentException("Unknown DATA message type: " + dm.getType());
        }
      case QUERY:
        return tm;
      case CONTROL:
        return tm;
      default:
        throw new IllegalArgumentException("Unknown message type: " + tm.getType().name());
    }
  }

}
