package edu.washington.escience.myria.column;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

/**
 * A column of String values, packed into a UTF-8 encoded byte array.
 * 
 * 
 */
public final class StringPackedColumn extends StringColumn {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** A read-only buffer containing the packed UTF-8 character data. */
  private final ByteString data;
  /** Contains the offset of each string in order. */
  private final int[] offsets;

  /**
   * Constructs a new column.
   * 
   * @param data buffer containing concatenated UTF-8 bytes of all strings
   * @param offsets starting byte offsets of strings within data buffer
   * */
  public StringPackedColumn(final ByteString data, final int[] offsets) {
    this.data = data;
    this.offsets = offsets;
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  @Override
  @Nonnull
  public String getString(final int row) {
    Preconditions.checkElementIndex(row, size());
    int len;
    if (row == offsets.length - 1) {
      len = data.size() - offsets[row];
    } else {
      len = offsets[row + 1] - offsets[row];
    }
    return data.substring(offsets[row], offsets[row] + len).toStringUtf8();
  }

  @Override
  public int size() {
    return offsets.length;
  }
}
