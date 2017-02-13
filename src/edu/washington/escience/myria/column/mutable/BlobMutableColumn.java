package edu.washington.escience.myria.column.mutable;

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.BlobColumn;
/**
 * A mutable column of Blob values.
 *
 */
public final class BlobMutableColumn extends MutableColumn<ByteBuffer> {
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
  public BlobMutableColumn(final ByteBuffer[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  @Deprecated
  public ByteBuffer getObject(final int row) {
    return getBlob(row);
  }

  @Override
  public ByteBuffer getBlob(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public Type getType() {
    return Type.BLOB_TYPE;
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
  public void replaceBlob(final ByteBuffer value, final int row) {
    Preconditions.checkElementIndex(row, size());
    data[row] = value;
  }

  @Override
  public BlobColumn toColumn() {
    return new BlobColumn(data.clone(), position);
  }

  @Override
  public BlobMutableColumn clone() {
    return new BlobMutableColumn(data.clone(), position);
  }
}
