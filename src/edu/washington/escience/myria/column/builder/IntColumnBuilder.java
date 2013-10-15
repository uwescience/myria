package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.IntBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.IntArrayColumn;
import edu.washington.escience.myria.column.IntColumn;
import edu.washington.escience.myria.column.IntProtoColumn;
import edu.washington.escience.myria.column.mutable.IntArrayMutableColumn;
import edu.washington.escience.myria.column.mutable.IntMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

/**
 * A column of Integer values.
 * 
 */
public final class IntColumnBuilder implements ColumnBuilder<Integer> {
  /** View of the column data as ints. */
  private final IntBuffer data;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public IntColumnBuilder() {
    data = IntBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  /**
   * copy.
   * 
   * @param data the underlying data
   * */
  private IntColumnBuilder(final IntBuffer data) {
    this.data = data;
  }

  /**
   * Constructs an IntColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static IntColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.INT_VALUE) {
      throw new IllegalArgumentException("Trying to construct IntColumn from non-INT ColumnMessage");
    }
    if (!message.hasIntColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type INT but no IntColumn");
    }
    return new IntProtoColumn(message.getIntColumn());
  }

  @Override
  public Type getType() {
    return Type.INT_TYPE;
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if the column is already full
   */
  public IntColumnBuilder append(final int value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    data.put(value);
    return this;
  }

  @Override
  public IntColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append((Integer) value);
  }

  @Override
  public IntColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(resultSet.getInt(jdbcIndex));
  }

  @Override
  public IntColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(statement.columnInt(index));
  }

  @Override
  public IntColumnBuilder append(final Integer value) {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(value.intValue());
  }

  @Override
  public int size() {
    return data.position();
  }

  @Override
  public IntColumn build() {
    built = true;
    return new IntArrayColumn(data.array(), data.position());
  }

  @Override
  public IntMutableColumn buildMutable() {
    built = true;
    return new IntArrayMutableColumn(data.array(), data.position());
  }

  @Override
  @Deprecated
  public IntColumnBuilder replace(final int idx, final Integer value) throws IndexOutOfBoundsException {
    return replace(idx, value.intValue());
  }

  /**
   * Replace the specified element.
   * 
   * @param value element to be inserted.
   * @param idx where to insert the element.
   * @return this column builder.
   * @throws IndexOutOfBoundsException if the idx exceeds the currently valid indices, i.e. the currently built size.
   */
  public IntColumnBuilder replace(final int idx, final int value) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(idx, data.position());
    data.put(idx, value);
    return this;
  }

  @Override
  public IntColumnBuilder expand(final int size) {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    data.position(data.position() + size);
    return this;
  }

  @Override
  public IntColumnBuilder expandAll() {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    data.position(data.capacity());
    return this;
  }

  @Override
  @Deprecated
  public Integer get(final int row) {
    return data.get(row);
  }

  /**
   * @param row the row to get
   * @return primitive value of the row
   * */
  public int getInt(final int row) {
    return data.get(row);
  }

  @Override
  public IntColumnBuilder forkNewBuilder() {
    int[] arr = new int[data.array().length];
    System.arraycopy(data.array(), 0, arr, 0, data.position());
    return new IntColumnBuilder((IntBuffer) IntBuffer.wrap(arr).position(data.position()).limit(data.limit()));
  }

}