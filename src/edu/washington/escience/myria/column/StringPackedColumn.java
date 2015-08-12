package edu.washington.escience.myria.column;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.google.common.base.Preconditions;

/**
 * A column of String values, packed into a UTF-8 encoded byte array.
 * 
 * 
 */
public final class StringPackedColumn extends StringColumn {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** A read-only buffer containing the packed UTF-8 character data. */
  private final ByteBuffer data;
  /** Contains the number of bytes in data. */
  private final int numBytes;
  /** Contains the offset of each string in order. */
  private final int[] offsets;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param offsets offsets of strings within data column
   * */
  public StringPackedColumn(final ByteBuffer data, final int numBytes, final int[] offsets) {
    this.data = data;
    this.numBytes = numBytes;
    this.offsets = offsets;
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
  public String getString(final int row) {
    Preconditions.checkElementIndex(row, size());
    int len;
    if (row == offsets.length - 1) {
      len = numBytes - offsets[row];
    } else {
      len = offsets[row + 1] - offsets[row];
    }
    byte[] strBytes = new byte[len];
    data.position(offsets[row]);
    data.get(strBytes, 0, len);
    return new String(strBytes, StandardCharsets.UTF_8);
  }

  @Override
  public int size() {
    return offsets.length;
  }
}
