package edu.washington.escience.myria.column;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.StringColumnMessage;
import edu.washington.escience.myria.util.ImmutableIntArray;

/**
 * A column of String values.
 * 
 * @author dhalperi
 * 
 */
public final class StringArrayColumn extends StringColumn {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Contains the packed character data. */
  private final String[] data;
  /** Number of elements in this column. */
  private final int numStrings;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numStrings number of tuples.
   * */
  public StringArrayColumn(final String[] data, final int numStrings) {
    this.data = data;
    this.numStrings = numStrings;
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
  public String getString(final int row) {
    return data[Preconditions.checkElementIndex(row, numStrings)];
  }

  @Override
  public ColumnMessage serializeToProto() {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final StringColumnMessage.Builder inner = StringColumnMessage.newBuilder();
    StringBuilder sb = new StringBuilder();
    int startP = 0, endP = 0;
    for (int i = 0; i < numStrings; i++) {
      endP = startP + data[i].length();
      inner.addStartIndices(startP);
      inner.addEndIndices(endP);
      sb.append(data[i]);
      startP = endP;
    }
    inner.setData(ByteString.copyFromUtf8(sb.toString()));

    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.STRING).setStringColumn(inner).build();
  }

  @Override
  public ColumnMessage serializeToProto(final ImmutableIntArray validIndices) {
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final StringColumnMessage.Builder inner = StringColumnMessage.newBuilder();
    StringBuilder sb = new StringBuilder();
    int startP = 0, endP = 0;
    for (int i : validIndices) {
      endP = startP + data[i].length();
      inner.addStartIndices(startP);
      inner.addEndIndices(endP);
      sb.append(data[i]);
      startP = endP;
    }
    inner.setData(ByteString.copyFromUtf8(sb.toString()));

    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.STRING).setStringColumn(inner).build();
  }

  @Override
  public int size() {
    return numStrings;
  }
}