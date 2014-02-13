package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.StringArrayColumn;
import edu.washington.escience.myria.column.StringColumn;
import edu.washington.escience.myria.column.mutable.StringMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.StringColumnMessage;

/**
 * A column of String values.
 * 
 */
public final class StringColumnBuilder extends ColumnBuilder<String> {

  /**
   * The internal representation of the data.
   * */
  private final String[] data;
  /** Number of elements in this column. */
  private int numStrings;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public StringColumnBuilder() {
    numStrings = 0;
    data = new String[TupleBatch.BATCH_SIZE];
  }

  /**
   * copy.
   * 
   * @param numStrings the actual num strings in the data
   * @param data the underlying data
   * */
  private StringColumnBuilder(final String[] data, final int numStrings) {
    this.numStrings = numStrings;
    this.data = data;
  }

  /**
   * Constructs a StringColumn by deserializing the given ColumnMessage.
   * 
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static StringColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.STRING_VALUE) {
      throw new IllegalArgumentException("Trying to construct StringColumn from non-STRING ColumnMessage");
    }
    if (!message.hasStringColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type STRING but no StringColumn");
    }
    final StringColumnMessage stringColumn = message.getStringColumn();
    List<Integer> startIndices = stringColumn.getStartIndicesList();
    List<Integer> endIndices = stringColumn.getEndIndicesList();
    String[] newData = new String[numTuples];
    String allStrings = stringColumn.getData().toStringUtf8();
    for (int i = 0; i < numTuples; i++) {
      newData[i] = allStrings.substring(startIndices.get(i), endIndices.get(i));
    }
    return new StringColumnBuilder(newData, numTuples).build();
  }

  @Override
  public StringColumnBuilder appendString(final String value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    if (numStrings >= TupleBatch.BATCH_SIZE) {
      throw new BufferOverflowException();
    }
    data[numStrings++] = value;
    return this;
  }

  @Override
  public Type getType() {
    return Type.STRING_TYPE;
  }

  @Override
  public StringColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return appendString(resultSet.getString(jdbcIndex));
  }

  @Override
  public StringColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index) throws SQLiteException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return appendString(statement.columnString(index));
  }

  @Override
  public int size() {
    return numStrings;
  }

  @Override
  public StringColumn build() {
    built = true;
    return new StringArrayColumn(data, numStrings);
  }

  @Override
  public StringMutableColumn buildMutable() {
    built = true;
    return new StringMutableColumn(data, numStrings);
  }

  @Override
  public StringColumnBuilder replace(final int idx, final String value) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(idx, numStrings);
    Preconditions.checkNotNull(value);
    data[idx] = value;
    return this;
  }

  @Override
  public StringColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    if (numStrings + size > data.length) {
      throw new BufferOverflowException();
    }
    numStrings += size;
    return this;
  }

  @Override
  public StringColumnBuilder expandAll() {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    numStrings = data.length;
    return this;
  }

  @Override
  public String getObject(final int row) {
    Preconditions.checkArgument(row >= 0 && row < data.length);
    return data[row];
  }

  @Override
  public String getString(final int row) {
    Preconditions.checkArgument(row >= 0 && row < data.length);
    return data[row];
  }

  @Deprecated
  @Override
  public ColumnBuilder<String> appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return appendString((String) value);
  }

  @Override
  public StringColumnBuilder forkNewBuilder() {
    String[] newData = new String[data.length];
    System.arraycopy(data, 0, newData, 0, numStrings);
    return new StringColumnBuilder(newData, numStrings);
  }

}