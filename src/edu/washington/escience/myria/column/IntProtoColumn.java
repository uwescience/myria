package edu.washington.escience.myria.column;

import java.nio.IntBuffer;

import com.google.protobuf.ByteString;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.IntColumnMessage;

/**
 * An IntColumn that simply wraps a read-only Protobuf message.
 *
 *
 */
public final class IntProtoColumn extends IntColumn {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The ByteString containing the data. Immutable. */
  private final ByteString columnData;
  /** The Protobuf message containing the int column. */
  private final IntBuffer intBuffer;

  /**
   * Construct a new IntProtoColumn wrapping the IntColumnMessage.
   *
   * @param message a Protobuf message containing a column of integers.
   */
  public IntProtoColumn(final IntColumnMessage message) {
    this(message.getData());
  }

  /**
   * Construct a new IntProtoColumn using the ByteString data.
   *
   * @param data a byte string of data.
   */
  public IntProtoColumn(final ByteString data) {
    columnData = data;
    intBuffer = columnData.asReadOnlyByteBuffer().asIntBuffer();
  }

  @Override
  public Integer getObject(final int row) {
    return Integer.valueOf(intBuffer.get(row));
  }

  @Override
  public int getInt(final int row) {
    return intBuffer.get(row);
  }

  @Override
  public ColumnMessage serializeToProto() {
    final IntColumnMessage.Builder inner = IntColumnMessage.newBuilder().setData(columnData);
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.INT).setIntColumn(inner).build();
  }

  @Override
  public int size() {
    return intBuffer.limit();
  }
}
