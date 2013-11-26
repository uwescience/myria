package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.LongBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.DateTimeColumn;
import edu.washington.escience.myria.column.mutable.DateTimeMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.DateTimeColumnMessage;

/**
 * A column of Date values.
 * 
 */
public final class DateTimeColumnBuilder implements ColumnBuilder<DateTime> {

  /**
   * The internal representation of the data.
   * */
  private final DateTime[] data;

  /** Number of elements in this column. */
  private int numDates;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public DateTimeColumnBuilder() {
    numDates = 0;
    data = new DateTime[TupleBatch.BATCH_SIZE];
  }

  /**
   * copy.
   * 
   * @param numDates the actual num strings in the data
   * @param data the underlying data
   * */
  private DateTimeColumnBuilder(final DateTime[] data, final int numDates) {
    this.numDates = numDates;
    this.data = data;
  }

  /**
   * Constructs a DateColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static DateTimeColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.DATETIME_VALUE) {
      throw new IllegalArgumentException("Trying to construct DateColumn from non-DATE ColumnMessage");
    }
    if (!message.hasDateColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type DATE but no DateColumn");
    }
    final DateTimeColumnMessage dateColumn = message.getDateColumn();
    DateTime[] newData = new DateTime[numTuples];
    LongBuffer data = dateColumn.getData().asReadOnlyByteBuffer().asLongBuffer();
    for (int i = 0; i < numTuples; i++) {
      newData[i] = new DateTime(data.get());
    }
    return new DateTimeColumnBuilder(newData, numTuples).build();
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  @Override
  public DateTimeColumnBuilder append(final DateTime value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    if (numDates >= TupleBatch.BATCH_SIZE) {
      throw new BufferOverflowException();
    }
    data[numDates++] = value;
    return this;
  }

  @Override
  public Type getType() {
    return Type.DATETIME_TYPE;
  }

  @Override
  public DateTimeColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(new DateTime(resultSet.getTimestamp(jdbcIndex).getTime()));
  }

  /**
   * SQLLite does not support date type. use long to store the difference, measured in milliseconds, between the date
   * time and midnight, January 1, 1970 UTC.
   * 
   * @param statement contains the results
   * @return this column builder.
   * @param index the position of the element to extract. 0-indexed.
   * @throws SQLiteException if there are SQLite errors.
   * @throws BufferOverflowException if the column is already full
   */
  @Override
  public DateTimeColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");

    return append(new DateTime(statement.columnLong(index)));
  }

  @Override
  public int size() {
    return numDates;
  }

  @Override
  public DateTimeColumn build() {
    built = true;
    return new DateTimeColumn(data, numDates);
  }

  @Override
  public DateTimeMutableColumn buildMutable() {
    built = true;
    return new DateTimeMutableColumn(data, numDates);
  }

  @Override
  public DateTimeColumnBuilder replace(final int idx, final DateTime value) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(idx, numDates);
    Preconditions.checkNotNull(value);
    data[idx] = value;
    return this;
  }

  @Override
  public DateTimeColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    if (numDates + size > data.length) {
      throw new BufferOverflowException();
    }
    numDates += size;
    return this;
  }

  @Override
  public DateTimeColumnBuilder expandAll() {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    numDates = data.length;
    return this;
  }

  @Override
  public DateTime get(final int row) {
    Preconditions.checkArgument(row >= 0 && row < data.length);
    return data[row];
  }

  @Override
  public DateTimeColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append((DateTime) value);
  }

  @Override
  public DateTimeColumnBuilder forkNewBuilder() {
    DateTime[] newData = new DateTime[data.length];
    System.arraycopy(data, 0, newData, 0, numDates);
    return new DateTimeColumnBuilder(newData, numDates);
  }

}