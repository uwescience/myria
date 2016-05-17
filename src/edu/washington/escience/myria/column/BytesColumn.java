/**
 *
 */
package edu.washington.escience.myria.column;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;

/**
 * 
 */
public class BytesColumn extends Column<ByteBuffer> {
  /**
   * @param bs
   * @param i
   */
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1;
  /** Internal representation of the column data. */
  private final ByteBuffer[] data;
  /** The number of existing rows in this column. */
  private final int position;

  public BytesColumn(final ByteBuffer[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  public ByteBuffer getByteBuffer(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];

  }

  @Override
  public @Nonnull ByteBuffer getObject(final int row) {
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

}
