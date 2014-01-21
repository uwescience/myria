package edu.washington.escience.myria.column;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.Objects;

import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.hash.Hasher;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.ImmutableIntArray;
import edu.washington.escience.myria.util.TypeFunnel;

/**
 * A column that holds a constant value.
 */
public class ConstantValueColumn extends Column<Comparable<?>> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The constant value of this column. */
  private final Comparable<?> value;
  /** The type of this column. */
  private final Type type;
  /** If this is a Boolean column, the primitive boolean value of the column. */
  private boolean booleanValue;
  /** If this is a DateTime column, the DateTime value of the column. */
  private DateTime dateTimeValue;
  /** If this is a Double column, the primitive double value of the column. */
  private double doubleValue;
  /** If this is a Float column, the primitive float value of the column. */
  private float floatValue;
  /** If this is an Integer column, the primitive int value of the column. */
  private int intValue;
  /** If this is a Long column, the primitive long value of the column. */
  private long longValue;
  /** If this is a String column, the primitive String value of the column. */
  private String stringValue;
  /** The number of rows in this column. */
  private final int size;

  /**
   * Instantiate a new ConstantValueColumn that returns the specified values of the specified time and has the specified
   * number of rows.
   * 
   * @param value the value of all rows in this column.
   * @param type the type of the value.
   * @param size the number of rows.
   */
  public ConstantValueColumn(final Comparable<?> value, final Type type, final int size) {
    this.value = Objects.requireNonNull(value, "value");
    this.type = Objects.requireNonNull(type, "type");
    this.size = size;
    switch (type) {
      case BOOLEAN_TYPE:
        booleanValue = (Boolean) value;
        break;
      case DATETIME_TYPE:
        dateTimeValue = (DateTime) value;
        break;
      case DOUBLE_TYPE:
        doubleValue = (Double) value;
        break;
      case FLOAT_TYPE:
        floatValue = (Float) value;
        break;
      case INT_TYPE:
        intValue = (Integer) value;
        break;
      case LONG_TYPE:
        longValue = (Long) value;
        break;
      case STRING_TYPE:
        stringValue = (String) value;
        break;
    }
  }

  @Override
  public void addToHasher(final int row, final Hasher hasher) {
    switch (getType()) {
      case BOOLEAN_TYPE:
        hasher.putBoolean(getBoolean(row));
        break;
      case DATETIME_TYPE:
        hasher.putObject(getDateTime(row), TypeFunnel.INSTANCE);
        break;
      case DOUBLE_TYPE:
        hasher.putDouble(getDouble(row));
        break;
      case FLOAT_TYPE:
        hasher.putFloat(getFloat(row));
        break;
      case INT_TYPE:
        hasher.putInt(getInt(row));
        break;
      case LONG_TYPE:
        hasher.putLong(getLong(row));
        break;
      case STRING_TYPE:
        hasher.putObject(getString(row), TypeFunnel.INSTANCE);
        break;
    }
  }

  @Override
  public void append(final int index, final ColumnBuilder<?> columnBuilder) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final int leftIdx, final Column<?> rightColumn, final int rightIdx) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Column<Comparable<?>> filter(final BitSet filter) {
    return new ConstantValueColumn(value, type, filter.cardinality());
  }

  @Override
  public boolean getBoolean(final int row) {
    if (type == Type.BOOLEAN_TYPE) {
      return booleanValue;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public DateTime getDateTime(final int row) {
    if (type == Type.DATETIME_TYPE) {
      return dateTimeValue;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public double getDouble(final int row) {
    if (type == Type.DOUBLE_TYPE) {
      return doubleValue;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloat(final int row) {
    if (type == Type.FLOAT_TYPE) {
      return floatValue;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public int getInt(final int row) {
    if (type == Type.INT_TYPE) {
      return intValue;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public void getIntoJdbc(final int row, final PreparedStatement statement, final int jdbcIndex) throws SQLException {
    switch (getType()) {
      case BOOLEAN_TYPE:
        statement.setBoolean(jdbcIndex, getBoolean(row));
        break;
      case DATETIME_TYPE:
        statement.setTimestamp(jdbcIndex, new Timestamp(getDateTime(row).getMillis()));
        break;
      case DOUBLE_TYPE:
        statement.setDouble(jdbcIndex, getDouble(row));
        break;
      case FLOAT_TYPE:
        statement.setFloat(jdbcIndex, getFloat(row));
        break;
      case INT_TYPE:
        statement.setInt(jdbcIndex, getInt(row));
        break;
      case LONG_TYPE:
        statement.setLong(jdbcIndex, getLong(row));
        break;
      case STRING_TYPE:
        statement.setString(jdbcIndex, getString(row));
        break;
    }
  }

  @Override
  public void getIntoSQLite(final int row, final SQLiteStatement statement, final int sqliteIndex)
      throws SQLiteException {
    switch (getType()) {
      case BOOLEAN_TYPE:
        /* In SQLite, booleans are integers represented as 0 (false) or 1 (true). */
        int colVal = 0;
        if (getBoolean(row)) {
          colVal = 1;
        }
        statement.bind(sqliteIndex, colVal);
        break;
      case DATETIME_TYPE:
        statement.bind(sqliteIndex, getDateTime(row).getMillis()); // SQLite long
        break;
      case DOUBLE_TYPE:
        statement.bind(sqliteIndex, getDouble(row));
        break;
      case FLOAT_TYPE:
        statement.bind(sqliteIndex, getFloat(row));
        break;
      case INT_TYPE:
        statement.bind(sqliteIndex, getInt(row));
        break;
      case LONG_TYPE:
        statement.bind(sqliteIndex, getLong(row));
        break;
      case STRING_TYPE:
        statement.bind(sqliteIndex, getString(row));
        break;
    }
  }

  @Override
  public long getLong(final int row) {
    if (type == Type.LONG_TYPE) {
      return longValue;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public String getString(final int row) {
    if (type == Type.STRING_TYPE) {
      return stringValue;
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public Comparable<?> getObject(final int row) {
    return value;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public ColumnMessage serializeToProto() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnMessage serializeToProto(final ImmutableIntArray validIndices) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return size;
  }
}
