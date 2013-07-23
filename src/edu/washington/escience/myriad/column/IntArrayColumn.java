package edu.washington.escience.myriad.column;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;
import edu.washington.escience.myriad.proto.DataProto.IntColumnMessage;

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
  IntArrayColumn(final int[] data, final int numData) {
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
  public int size() {
    return position;
  }
}