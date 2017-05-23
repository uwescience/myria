package edu.washington.escience.myria;

import java.io.Serializable;

import org.joda.time.DateTime;
import java.nio.ByteBuffer;

import edu.washington.escience.myria.column.Column;

/**
 * Class representing a type in Myria. Types are static objects defined by this class; hence, the Type constructor is
 * private.
 */
public enum Type implements Serializable {
  /**
   * int type.
   * */
  INT_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> intColumn,
        final int tupleIndex,
        final Object operand) {
      final int v = intColumn.getInt(tupleIndex);
      return compare(op, v, (Integer) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + column.getInt(tupleIndex);
    }

    @Override
    public Integer fromString(final String str) {
      return Integer.valueOf(str);
    }

    @Override
    public Class<?> toJavaType() {
      return int.class;
    }

    @Override
    public Class<?> toJavaArrayType() {
      return int[].class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return Integer.class;
    }

    @Override
    public String getName() {
      return "Int";
    }

    @Override
    public String getEnumName() {
      return "INT_TYPE";
    }
  },

  /**
   * float type.
   * */
  FLOAT_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> floatColumn,
        final int tupleIndex,
        final Object operand) {
      final float v = floatColumn.getFloat(tupleIndex);
      return compare(op, v, (Float) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + column.getFloat(tupleIndex);
    }

    @Override
    public Float fromString(final String str) {
      return Float.valueOf(str);
    }

    @Override
    public Class<?> toJavaType() {
      return float.class;
    }

    @Override
    public Class<?> toJavaArrayType() {
      return float[].class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return Float.class;
    }

    @Override
    public String getName() {
      return "Float";
    }

    @Override
    public String getEnumName() {
      return "FLOAT_TYPE";
    }
  },

  /**
   * Double type.
   * */
  DOUBLE_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> doubleColumn,
        final int tupleIndex,
        final Object operand) {
      final double v = doubleColumn.getDouble(tupleIndex);
      return compare(op, v, (Double) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + column.getDouble(tupleIndex);
    }

    @Override
    public Double fromString(final String str) {
      return Double.valueOf(str);
    }

    @Override
    public Class<?> toJavaType() {
      return double.class;
    }

    @Override
    public Class<?> toJavaArrayType() {
      return double[].class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return Double.class;
    }

    @Override
    public String getName() {
      return "Double";
    }

    public String getEnumName() {
      return "DOUBLE_TYPE";
    }
  },

  /**
   * Boolean type.
   * */
  BOOLEAN_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> booleanColumn,
        final int tupleIndex,
        final Object operand) {
      final boolean v = booleanColumn.getBoolean(tupleIndex);
      return compare(op, v, (Boolean) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return column.getBoolean(tupleIndex) + "";
    }

    @Override
    public Boolean fromString(final String str) {
      return Boolean.valueOf(str);
    }

    @Override
    public Class<?> toJavaType() {
      return boolean.class;
    }

    @Override
    public Class<?> toJavaArrayType() {
      return boolean[].class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return Boolean.class;
    }

    @Override
    public String getName() {
      return "Boolean";
    }

    @Override
    public String getEnumName() {
      return "BOOLEAN_TYPE";
    }
  },

  /**
   * String type.
   * */
  STRING_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> stringColumn,
        final int tupleIndex,
        final Object operand) {
      final String string = stringColumn.getString(tupleIndex);
      return compare(op, string, (String) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return column.getString(tupleIndex);
    }

    @Override
    public String fromString(final String str) {
      return str;
    }

    @Override
    public Class<?> toJavaType() {
      return String.class;
    }

    @Override
    public Class<?> toJavaArrayType() {
      return String[].class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return toJavaType();
    }

    @Override
    public String getName() {
      return "String";
    }

    @Override
    public String getEnumName() {
      return "STRING_TYPE";
    }
  },

  /**
   * Long type.
   * */
  LONG_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> longColumn,
        final int tupleIndex,
        final Object operand) {
      final long v = longColumn.getLong(tupleIndex);
      return compare(op, v, (Long) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + column.getLong(tupleIndex);
    }

    @Override
    public Long fromString(final String str) {
      return Long.valueOf(str);
    }

    @Override
    public Class<?> toJavaType() {
      return long.class;
    }

    @Override
    public Class<?> toJavaArrayType() {
      return long[].class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return Long.class;
    }

    @Override
    public String getName() {
      return "Long";
    }

    @Override
    public String getEnumName() {
      return "LONG_TYPE";
    }
  },
  /**
   * byteArray type.
   * */
  BLOB_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> bytesColumn,
        final int tupleIndex,
        final Object operand) {
      final ByteBuffer v = bytesColumn.getBlob(tupleIndex);
      return compare(op, v, (ByteBuffer) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {

      return "" + column.getBlob(tupleIndex);
    }

    @Override
    public String getName() {
      return "Blob";
    }

    @Override
    public String getEnumName() {
      return "BLOB_TYPE";
    }

    @Override
    public Class<?> toJavaArrayType() {
      return ByteBuffer[].class;
    }

    @Override
    public Class<?> toJavaType() {
      return ByteBuffer.class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return ByteBuffer.class;
    }
  },

  /**
   * date type.
   * */
  DATETIME_TYPE() {
    @Override
    public boolean filter(
        final SimplePredicate.Op op,
        final Column<?> dateColumn,
        final int tupleIndex,
        final Object operand) {
      final DateTime v = dateColumn.getDateTime(tupleIndex);
      return compare(op, v, (DateTime) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + column.getDateTime(tupleIndex);
    }

    @Override
    public DateTime fromString(final String str) {
      return DateTime.parse(str);
    }

    @Override
    public Class<?> toJavaType() {
      return DateTime.class;
    }

    @Override
    public Class<?> toJavaArrayType() {
      return DateTime[].class;
    }

    @Override
    public Class<?> toJavaObjectType() {
      return toJavaType();
    }

    @Override
    public String getName() {
      return "DateTime";
    }

    @Override
    public String getEnumName() {
      return "DATETIME_TYPE";
    }
  };

  /**
   * @param op the operation.
   * @param column the data column.
   * @param tupleIndex the index.
   * @param operand the operand constant.
   * @return true if the #tupleIndex tuple in column satisfy the predicate, else false.
   * */
  public abstract boolean filter(
      SimplePredicate.Op op, Column<?> column, int tupleIndex, Object operand);

  /**
   * @return A string representation of the #tupleIndex value in column.
   * @param column the columns
   * @param tupleIndex the index
   * */
  public abstract String toString(final Column<?> column, final int tupleIndex);

  /**
   * Create an instance of Type from a String.
   *
   * @param str The string to convert
   * @return An object of an appropriate type
   */
  public Comparable<?> fromString(final String str) {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the value 0 if <code>x</code> is numerically equal to <code>y</code>; a value less than 0 if <code>x</code>
   *         is numerically less than <code>y</code>; and a value greater than 0 otherwise.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final int x, final int y) {
    return Integer.compare(x, y);
  }

  /**
   * @return the value 0 if x == y; a value less than 0 if !x && y; and a value greater than 0 if x && !y.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final boolean x, final boolean y) {
    return Boolean.compare(x, y);
  }

  /**
   * @return negative value if this is less, 0 if equal, or positive value if greater.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final DateTime x, final DateTime y) {
    return x.compareTo(y);
  }

  /**
   * @return the value 0 if <code>x</code> is numerically equal to <code>y</code>; a value less than 0 if <code>x</code>
   *         is numerically less than <code>y</code>; and a value greater than 0 otherwise.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final double x, final double y) {
    return Double.compare(x, y);
  }

  /**
   * @return the value 0 if <code>x</code> is numerically equal to <code>y</code>; a value less than 0 if <code>x</code>
   *         is numerically less than <code>y</code>; and a value greater than 0 otherwise.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final float x, final float y) {
    return Float.compare(x, y);
  }

  /**
   * @return the value 0 if <code>x</code> is numerically equal to <code>y</code>; a value less than 0 if <code>x</code>
   *         is numerically less than <code>y</code>; and a value greater than 0 otherwise.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final long x, final long y) {
    return Long.compare(x, y);
  }
  /**
   * @return the value 0 if <code>x</code> is numerically equal to <code>y</code>; a value less than 0 if <code>x</code>
   *         is numerically less than <code>y</code>; and a value greater than 0 otherwise.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final ByteBuffer x, final ByteBuffer y) {
    return x.compareTo(y);
  }

  /**
   * @return value 0 if the <code>x</code> is equal to <code>y</code>; a value less than 0 if <code>x</code> is
   *         lexicographically less than <code>y</code>; and a value greater than 0 if this string is otherwise.
   * @param x the value to be compared in a tuple
   * @param y the operand
   * */
  public static final int compareRaw(final String x, final String y) {
    return x.compareTo(y);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(
      final SimplePredicate.Op op, final long valueInTuple, final long operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(
      final SimplePredicate.Op op, final double valueInTuple, final double operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(
      final SimplePredicate.Op op, final boolean valueInTuple, final boolean operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(
      final SimplePredicate.Op op, final DateTime valueInTuple, final DateTime operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(
      final SimplePredicate.Op op, final int valueInTuple, final int operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }
  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(
      final SimplePredicate.Op op, final ByteBuffer valueInTuple, final ByteBuffer operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(
      final SimplePredicate.Op op, final String valueInTuple, final String operand) {
    switch (op) {
      case LIKE:
        return valueInTuple.indexOf(operand) >= 0;
      default:
        int compared = compareRaw(valueInTuple, operand);
        return evalOp(op, compared);
    }
  }

  /**
   * Given an int that is the output of a <code>compareTo</code> function, return true if that comparison value
   * satisfies that operator.
   *
   * @param op the comparison operator.
   * @param compared the comparison value, output using <code>compareTo</code> semantics.
   * @return true if that comparison value satisfies the operator.
   */
  private static boolean evalOp(final SimplePredicate.Op op, final int compared) {
    switch (op) {
      case EQUALS:
        return compared == 0;

      case NOT_EQUALS:
        return compared != 0;

      case GREATER_THAN:
        return compared > 0;

      case GREATER_THAN_OR_EQ:
        return compared >= 0;

      case LESS_THAN:
        return compared < 0;

      case LESS_THAN_OR_EQ:
        return compared <= 0;

      case LIKE:
        throw new UnsupportedOperationException();
    }

    return false;
  }

  /**
   * @param c the Java class to be converted to a Myria Type
   * @return the associated Myria Type for a Java class
   */
  public static Type fromJavaType(Class<?> c) {
    if (c == null) {
      return null;
    } else if (c.equals(Integer.class) || c.equals(int.class)) {
      return INT_TYPE;
    } else if (c.equals(Float.class) || c.equals(float.class)) {
      return FLOAT_TYPE;
    } else if (c.equals(Double.class) || c.equals(double.class)) {
      return DOUBLE_TYPE;
    } else if (c.equals(Boolean.class) || c.equals(boolean.class)) {
      return BOOLEAN_TYPE;
    } else if (c.equals(String.class)) {
      return STRING_TYPE;
    } else if (c.equals(Long.class) || c.equals(long.class)) {
      return LONG_TYPE;
    } else if (c.equals(ByteBuffer.class)) {
      return BLOB_TYPE;
    } else if (c.equals(DateTime.class)) {
      return DATETIME_TYPE;
    } else {
      return null;
    }
  }
  /**
   * @return the java type
   */
  public abstract Class<?> toJavaType();

  /**
   * @return the java array type
   */
  public abstract Class<?> toJavaArrayType();

  /**
   * @return the non primitive java type
   */
  public abstract Class<?> toJavaObjectType();

  /**
   * @return The name of the type usually used in the code. May be used to generate function names.
   */
  public abstract String getName();

  /**
   * @return The name of the enum type usually used in the code. May be used to generate function names.
   */
  public abstract String getEnumName();
}
