package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.BytesColumn;
import edu.washington.escience.myria.column.mutable.BytesMutableColumn;
import edu.washington.escience.myria.proto.DataProto.BytesColumnMessage;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of ByteBuffer values.
 * 
 */
public final class BytesColumnBuilder extends ColumnBuilder<ByteBuffer> {

  /**
   * The internal representation of the data.
   * */
  private final ByteBuffer[] data;

  /** Number of elements in this column. */
  private int numBB;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public BytesColumnBuilder() {
    numBB = 0;
    data = new ByteBuffer[TupleBatch.BATCH_SIZE];
  }

  /**
   * copy.
   * 
   * @param numBB the actual num strings in the data
   * @param data the underlying data
   * */
  private BytesColumnBuilder(final ByteBuffer[] data, final int numBB) {
    this.numBB = numBB;
    this.data = data;
  }

  /*
   * Constructs a BytesColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * 
   * @param numTuples num tuples in the column message
   * 
   * @return the built column
   */
  public static BytesColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    Preconditions.checkArgument(message.getType().ordinal() == ColumnMessage.Type.BYTES_VALUE,
        "Trying to construct Bytes from non-bytes ColumnMessage %s", message.getType());

    Preconditions.checkArgument(message.hasBytesColumn(), "ColumnMessage is missing BytesColumn");
    final BytesColumnMessage BytesColumn = message.getBytesColumn();
    ByteBuffer[] newData = new ByteBuffer[numTuples];
    ByteBuffer data = BytesColumn.getData().asReadOnlyByteBuffer();
    for (int i = 0; i < numTuples; i++) {
      // TODO: check: do I need to copy the data here?

      newData[i] = ByteBuffer.allocate(data.capacity());
      newData[i].put(data);
      // data;
    }

    return new BytesColumnBuilder(newData, numTuples).build();

  }

  @Override
  public BytesColumnBuilder appendByteBuffer(final ByteBuffer value) throws BufferOverflowException {
    Preconditions.checkState(!built, "No further changes are allowed after the builder has built the column.");
    Objects.requireNonNull(value, "value");
    if (numBB >= TupleBatch.BATCH_SIZE) {
      throw new BufferOverflowException();
    }
    data[numBB++] = value;
    return this;
  }

  @Override
  public Type getType() {
    return Type.BYTES_TYPE;
  }

  @Override
  public BytesColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException,
      BufferOverflowException {
    Preconditions.checkState(!built, "No further changes are allowed after the builder has built the column.");
    // TODO: fix
    return appendByteBuffer(ByteBuffer.wrap(resultSet.getBytes(jdbcIndex)));
  }

  @Override
  public BytesColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException,
      BufferOverflowException {
    Preconditions.checkState(!built, "No further changes are allowed after the builder has built the column.");

    return appendByteBuffer(ByteBuffer.wrap(statement.columnBlob(index)));
  }

  @Override
  public int size() {
    return numBB;
  }

  @Override
  public BytesColumn build() {
    built = true;
    return new BytesColumn(data, numBB);
  }

  @Override
  public BytesMutableColumn buildMutable() {
    built = true;
    return new BytesMutableColumn(data, numBB);
  }

  @Override
  public void replaceByteBuffer(final ByteBuffer value, final int row) throws IndexOutOfBoundsException {
    Preconditions.checkState(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, numBB);
    Preconditions.checkNotNull(value);
    data[row] = value;
  }

  @Override
  public BytesColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkState(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    if (numBB + size > data.length) {
      throw new BufferOverflowException();
    }
    numBB += size;
    return this;
  }

  @Override
  public BytesColumnBuilder expandAll() {
    Preconditions.checkState(!built, "No further changes are allowed after the builder has built the column.");
    numBB = data.length;
    return this;
  }

  @Override
  public ByteBuffer getByteBuffer(final int row) {
    Preconditions.checkElementIndex(row, numBB);
    return data[row];
  }

  @Override
  public ByteBuffer getObject(final int row) {
    return getByteBuffer(row);
  }

  @Deprecated
  @Override
  public ColumnBuilder<ByteBuffer> appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkState(!built, "No further changes are allowed after the builder has built the column.");
    return appendByteBuffer((ByteBuffer) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public BytesColumnBuilder forkNewBuilder() {
    ByteBuffer[] newData = new ByteBuffer[data.length];
    System.arraycopy(data, 0, newData, 0, numBB);
    return new BytesColumnBuilder(newData, numBB);
  }

}