package edu.washington.escience.myria.column;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;

public class BlobColumn extends Column<ByteBuffer> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1;
  /** Internal representation of the column data. */
  private final ByteBuffer[] data;
  /** The number of existing rows in this column. */
  private final int position;
  /**
   * Blob column  for binary data.
   * @param data in form of byteBuffer
   * @param numData
   */
  public BlobColumn(final ByteBuffer[] data, final int numData) {
    this.data = data;
    position = numData;
  }

  @Override
  public ByteBuffer getBlob(final int row) {
    Preconditions.checkElementIndex(row, position);
    return data[row];
  }

  @Override
  public @Nonnull ByteBuffer getObject(final int row) {
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
}
