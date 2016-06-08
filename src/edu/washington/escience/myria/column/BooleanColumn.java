package edu.washington.escience.myria.column;

import java.util.BitSet;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.proto.DataProto.BooleanColumnMessage;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.ImmutableBitSet;

/**
 * A column of Boolean values. To save space, this implementation uses a BitSet as the internal representation.
 *
 *
 */
public final class BooleanColumn extends Column<Boolean> {
  /** Required for Java Serialization. */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final BitSet data;
  /** Number of valid elements. */
  private final int numBits;

  /**
   * @param data the data
   * @param size the size of this column;
   * */
  public BooleanColumn(final BitSet data, final int size) {
    this.data = new ImmutableBitSet(data);
    numBits = size;
  }

  @Override
  public Boolean getObject(final int row) {
    return Boolean.valueOf(getBoolean(row));
  }

  /**
   * Returns the element at the specified row in this column.
   *
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
  public boolean getBoolean(final int row) {
    Preconditions.checkElementIndex(row, numBits);
    return data.get(row);
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final BooleanColumnMessage.Builder inner =
        BooleanColumnMessage.newBuilder().setData(ByteString.copyFrom(data.toByteArray()));
    return ColumnMessage.newBuilder()
        .setType(ColumnMessage.Type.BOOLEAN)
        .setBooleanColumn(inner)
        .build();
  }

  @Override
  public int size() {
    return numBits;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(data.get(i));
    }
    sb.append(']');
    return sb.toString();
  }
}
