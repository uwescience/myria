/**
 *
 */

package edu.washington.escience.myria.column.mutable;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.BytesColumn;

/**
 * A mutable column of byteBuffer values.
 * 
 */
public final class BytesMutableColumn extends MutableColumn<ByteBuffer> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Internal representation of the column data. */
  private final ByteBuffer[] data;
  /** The number of existing rows in this column. */
  private final int position;

  /**
   * Constructs a new column.
   * 
   * @param data the data
   * @param numData number of tuples.
   * */
  public BytesMutableColumn(final ByteBuffer[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  @Deprecated
  public ByteBuffer getObject(final int row) {
    return getByteBuffer(row);
  }

  @Override
  public ByteBuffer getByteBuffer(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.BYTES_TYPE;
  }

  @Override
  public int size() {
    return position;
  }

  @Override
  public String toString() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void replaceByteBuffer(final ByteBuffer value, final int row) {
    Preconditions.checkElementIndex(row, size());
    data[row] = value;
  }

  @Override
  public BytesColumn toColumn() {
    return new BytesColumn(data.clone(), position);
  }

  @Override
  public BytesMutableColumn clone() {
    return new BytesMutableColumn(data.clone(), position);
  }
}