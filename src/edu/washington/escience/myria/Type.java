package edu.washington.escience.myria;

import java.io.Serializable;

import org.joda.time.DateTime;

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
    public boolean filter(final SimplePredicate.Op op, final Column<?> intColumn, final int tupleIndex,
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

  },

  /**
   * float type.
   * */
  FLOAT_TYPE() {
    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> floatColumn, final int tupleIndex,
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
  },
  /**
   * Double type.
   * */
  DOUBLE_TYPE() {
    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> doubleColumn, final int tupleIndex,
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
  },
  /**
   * Boolean type.
   * */
  BOOLEAN_TYPE() {
    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> booleanColumn, final int tupleIndex,
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
  },

  /**
   * String type.
   * */
  STRING_TYPE() {
    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> stringColumn, final int tupleIndex,
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
  },
  /**
   * Long type.
   * */
  LONG_TYPE() {
    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> longColumn, final int tupleIndex,
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
  },

  /**
   * date type.
   * */
  DATETIME_TYPE() {
    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> dateColumn, final int tupleIndex,
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

  };

  /**
   * @param op the operation.
   * @param column the data column.
   * @param tupleIndex the index.
   * @param operand the operand constant.
   * @return true if the #tupleIndex tuple in column satisfy the predicate, else false.
   * */
  public abstract boolean filter(SimplePredicate.Op op, Column<?> column, int tupleIndex, Object operand);

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
   * @return true if valueInTuple op operand.
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * */
  public static final int compareRaw(final int valueInTuple, final int operand) {
    return Integer.compare(valueInTuple, operand);
  }

  /**
   * @return true if valueInTuple op operand.
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * */
  public static final int compareRaw(final boolean valueInTuple, final boolean operand) {
    return Boolean.compare(valueInTuple, operand);
  }

  /**
   * @return true if valueInTuple op operand.
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * */
  public static final int compareRaw(final DateTime valueInTuple, final DateTime operand) {
    return valueInTuple.compareTo(operand);
  }

  /**
   * @return true if valueInTuple op operand.
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * */
  public static final int compareRaw(final double valueInTuple, final double operand) {
    return Double.compare(valueInTuple, operand);
  }

  /**
   * @return true if valueInTuple op operand.
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * */
  public static final int compareRaw(final float valueInTuple, final float operand) {
    return Float.compare(valueInTuple, operand);
  }

  /**
   * @return true if valueInTuple op operand.
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * */
  public static final int compareRaw(final long valueInTuple, final long operand) {
    return Long.compare(valueInTuple, operand);
  }

  /**
   * @return true if valueInTuple op operand.
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * */
  public static final int compareRaw(final String valueInTuple, final String operand) {
    return valueInTuple.compareTo(operand);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(final SimplePredicate.Op op, final long valueInTuple, final long operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(final SimplePredicate.Op op, final double valueInTuple, final double operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(final SimplePredicate.Op op, final boolean valueInTuple, final boolean operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(final SimplePredicate.Op op, final DateTime valueInTuple, final DateTime operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(final SimplePredicate.Op op, final int valueInTuple, final int operand) {
    int compared = compareRaw(valueInTuple, operand);
    return evalOp(op, compared);
  }

  /**
   * @param op the operation
   * @param valueInTuple the value to be compared in a tuple
   * @param operand the operand
   * @return true if valueInTuple op operand.
   */
  private static boolean compare(final SimplePredicate.Op op, final String valueInTuple, final String operand) {
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
   * @return the java type
   */
  public abstract Class<?> toJavaType();
}
