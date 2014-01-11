package edu.washington.escience.myria.column;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.IntColumnMessage;
import edu.washington.escience.myria.util.ImmutableIntArray;

/**
 * A column of Int values.
 * 
 * @author dhalperi
 * 
 */
public final class IntArrayColumn extends IntColumn {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final int[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numData number of tuples.
   * */
  public IntArrayColumn(final int[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  public Integer get(final int row) {
    return Integer.valueOf(getInt(row));
  }

  @Override
  public int getInt(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public ColumnMessage serializeToProto() {
    ByteBuffer dataBytes = ByteBuffer.allocate(position * Integer.SIZE / Byte.SIZE);
    for (int i = 0; i < position; i++) {
      dataBytes.putInt(data[i]);
    }

    dataBytes.flip();
    final IntColumnMessage.Builder inner = IntColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.INT).setIntColumn(inner).build();
  }

  @Override
  public ColumnMessage serializeToProto(final ImmutableIntArray validIndices) {
    ByteBuffer dataBytes = ByteBuffer.allocate(validIndices.length() * Integer.SIZE / Byte.SIZE);
    for (int i : validIndices) {
      dataBytes.putInt(data[i]);
    }

    dataBytes.flip();
    final IntColumnMessage.Builder inner = IntColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.INT).setIntColumn(inner).build();
  }

  @Override
  public int size() {
    return position;
  }
}