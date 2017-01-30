package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.BlobColumn;
import edu.washington.escience.myria.column.mutable.BlobMutableColumn;
import edu.washington.escience.myria.proto.DataProto.BlobColumnMessage;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.MyriaUtils;
/**
 * A column of Blob values.
 *
 */
public final class BlobColumnBuilder extends ColumnBuilder<ByteBuffer> {

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
  public BlobColumnBuilder() {
    numBB = 0;
    data = new ByteBuffer[TupleUtils.getBatchSize(Type.BLOB_TYPE)];
  }

  /**
   * copy.
   *
   * @param numDates the actual num strings in the data
   * @param data the underlying data
   * */
  private BlobColumnBuilder(final ByteBuffer[] data, final int numBB) {
    this.numBB = numBB;
    this.data = data;
  }

  /*
   * Constructs a BlobColumn by deserializing the given ColumnMessage.
   *
   * @param message a ColumnMessage containing the contents of this column.
   *
   * @param numTuples num tuples in the column message
   *
   * @return the built column
   */
  public static BlobColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    Preconditions.checkArgument(
        message.getType().ordinal() == ColumnMessage.Type.BLOB_VALUE,
        "Trying to construct BlobColumn from non-bytes ColumnMessage %s",
        message.getType());

    Preconditions.checkArgument(message.hasBlobColumn(), "ColumnMessage is missing BlobColumn");
    final BlobColumnMessage BlobColumn = message.getBlobColumn();

    List<Integer> startIndices = BlobColumn.getStartIndicesList();
    List<Integer> endIndices = BlobColumn.getEndIndicesList();

    ByteBuffer[] newData = new ByteBuffer[numTuples];
    ByteBuffer data = BlobColumn.getData().asReadOnlyByteBuffer();
    for (int i = 0; i < numTuples; i++) {
      int length = endIndices.get(i) - startIndices.get(i);
      newData[i] = ByteBuffer.allocate(length);
      data.get(newData[i].array(), 0, length);
    }
    return new BlobColumnBuilder(newData, numTuples).build();
  }

  @Override
  public BlobColumnBuilder appendBlob(final ByteBuffer value) throws BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    Objects.requireNonNull(value, "value");
    if (numBB >= TupleUtils.getBatchSize(Type.BLOB_TYPE)) {
      throw new BufferOverflowException();
    }
    data[numBB++] = value;
    return this;
  }

  @Override
  public Type getType() {
    return Type.BLOB_TYPE;
  }

  @Override
  public BlobColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex)
      throws SQLException, BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendBlob(ByteBuffer.wrap(resultSet.getBytes(jdbcIndex)));
  }

  @Override
  public BlobColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");

    return appendBlob(ByteBuffer.wrap(statement.columnBlob(index)));
  }

  @Override
  public int size() {
    return numBB;
  }

  @Override
  public BlobColumn build() {
    built = true;
    return new BlobColumn(data, numBB);
  }

  @Override
  public BlobMutableColumn buildMutable() {
    built = true;
    return new BlobMutableColumn(data, numBB);
  }

  @Override
  public void replaceBlob(final ByteBuffer value, final int row) throws IndexOutOfBoundsException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, numBB);
    Preconditions.checkNotNull(value);
    data[row] = value;
  }

  @Override
  public BlobColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    if (numBB + size > data.length) {
      throw new BufferOverflowException();
    }
    numBB += size;
    return this;
  }

  @Override
  public BlobColumnBuilder expandAll() {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    numBB = data.length;
    return this;
  }

  @Override
  public ByteBuffer getBlob(final int row) {
    Preconditions.checkElementIndex(row, numBB);
    return data[row];
  }

  @Override
  public ByteBuffer getObject(final int row) {
    return getBlob(row);
  }

  @Deprecated
  @Override
  public ColumnBuilder<ByteBuffer> appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendBlob((ByteBuffer) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public BlobColumnBuilder forkNewBuilder() {
    ByteBuffer[] newData = new ByteBuffer[data.length];
    System.arraycopy(data, 0, newData, 0, numBB);
    return new BlobColumnBuilder(newData, numBB);
  }
}
