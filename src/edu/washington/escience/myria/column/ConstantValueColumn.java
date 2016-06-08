package edu.washington.escience.myria.column;

import java.util.BitSet;
import java.util.Objects;

import org.joda.time.DateTime;

import edu.washington.escience.myria.Type;

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
  public int size() {
    return size;
  }
}
