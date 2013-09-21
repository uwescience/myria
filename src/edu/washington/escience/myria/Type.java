package edu.washington.escience.myria;

import java.io.Serializable;

import org.joda.time.DateTime;

import edu.washington.escience.myria.column.BooleanColumn;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.DateTimeColumn;
import edu.washington.escience.myria.column.DoubleColumn;
import edu.washington.escience.myria.column.FloatColumn;
import edu.washington.escience.myria.column.IntColumn;
import edu.washington.escience.myria.column.LongColumn;
import edu.washington.escience.myria.column.StringColumn;

/**
 * Class representing a type in Myria. Types are static objects defined by this class; hence, the Type constructor is
 * private.
 */
public enum Type implements Serializable {
  /**
   * int type.
   * */
  INT_TYPE() {
    /**
     * @return true if valueInTuple op operand.
     * @param op the operation
     * @param valueInTuple the value to be compared in a tuple
     * @param operand the operand
     * */
    public boolean compare(final SimplePredicate.Op op, final int valueInTuple, final int operand) {
      switch (op) {
        case EQUALS:
          return valueInTuple == operand;
        case NOT_EQUALS:
          return valueInTuple != operand;

        case GREATER_THAN:
          return valueInTuple > operand;

        case GREATER_THAN_OR_EQ:
          return valueInTuple >= operand;

        case LESS_THAN:
          return valueInTuple < operand;

        case LESS_THAN_OR_EQ:
          return valueInTuple <= operand;

        case LIKE:
          return valueInTuple == operand;
      }

      return false;
    }

    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> intColumn, final int tupleIndex,
        final Object operand) {
      final int v = ((IntColumn) intColumn).getInt(tupleIndex);
      return compare(op, v, (Integer) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + ((IntColumn) column).getInt(tupleIndex);
    }

    @Override
    public Object fromString(final String str) {
      return Integer.valueOf(str);
    }
  },

  /**
   * float type.
   * */
  FLOAT_TYPE() {
    /**
     * @return true if valueInTuple op operand.
     * @param op the operation
     * @param valueInTuple the value to be compared in a tuple
     * @param operand the operand
     * */
    public boolean compare(final SimplePredicate.Op op, final float valueInTuple, final float operand) {
      int compVal = Float.compare(valueInTuple, operand);
      switch (op) {
        case EQUALS:
          return compVal == 0;
        case NOT_EQUALS:
          return compVal != 0;

        case GREATER_THAN:
          return compVal > 0;

        case GREATER_THAN_OR_EQ:
          return compVal >= 0;

        case LESS_THAN:
          return compVal < 0;

        case LESS_THAN_OR_EQ:
          return compVal <= 0;

        case LIKE:
          return compVal == 0;
      }

      return false;
    }

    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> floatColumn, final int tupleIndex,
        final Object operand) {
      final float v = ((FloatColumn) floatColumn).getFloat(tupleIndex);
      return compare(op, v, (Float) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + ((FloatColumn) column).getFloat(tupleIndex);
    }

    @Override
    public Object fromString(final String str) {
      return Float.valueOf(str);
    }
  },
  /**
   * Double type.
   * */
  DOUBLE_TYPE() {
    /**
     * @return true if valueInTuple op operand.
     * @param op the operation
     * @param valueInTuple the value to be compared in a tuple
     * @param operand the operand
     * */
    public boolean compare(final SimplePredicate.Op op, final double valueInTuple, final double operand) {
      int compVal = Double.compare(valueInTuple, operand);
      switch (op) {
        case EQUALS:
          return compVal == 0;
        case NOT_EQUALS:
          return compVal != 0;
        case GREATER_THAN:
          return compVal > 0;
        case GREATER_THAN_OR_EQ:
          return compVal >= 0;
        case LESS_THAN:
          return compVal < 0;
        case LESS_THAN_OR_EQ:
          return compVal <= 0;
        case LIKE:
          return compVal == 0;
      }

      return false;
    }

    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> doubleColumn, final int tupleIndex,
        final Object operand) {
      final double v = ((DoubleColumn) doubleColumn).getDouble(tupleIndex);
      return compare(op, v, (Double) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + ((DoubleColumn) column).getDouble(tupleIndex);
    }

    @Override
    public Object fromString(final String str) {
      return Double.valueOf(str);
    }
  },
  /**
   * Boolean type.
   * */
  BOOLEAN_TYPE() {
    /**
     * @return true if valueInTuple op operand.
     * @param op the operation
     * @param valueInTuple the value to be compared in a tuple
     * @param operand the operand
     * */
    public boolean compare(final SimplePredicate.Op op, final boolean valueInTuple, final boolean operand) {
      switch (op) {
        case EQUALS:
          return valueInTuple == operand;
        case NOT_EQUALS:
          return valueInTuple != operand;
        case LIKE:
          return valueInTuple == operand;

        case GREATER_THAN:
        case GREATER_THAN_OR_EQ:
        case LESS_THAN:
        case LESS_THAN_OR_EQ:
          throw new UnsupportedOperationException("BOOLEAN_TYPE not == or != or LIKE");
      }

      return false;
    }

    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> booleanColumn, final int tupleIndex,
        final Object operand) {
      final boolean v = ((BooleanColumn) booleanColumn).getBoolean(tupleIndex);
      return compare(op, v, (Boolean) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return ((BooleanColumn) column).getBoolean(tupleIndex) + "";
    }

    @Override
    public Object fromString(final String str) {
      return Boolean.valueOf(str);
    }
  },

  /**
   * String type.
   * */
  STRING_TYPE() {
    /**
     * @return true if valueInTuple op operand.
     * @param op the operation
     * @param valInTuple the value to be compared in a tuple
     * @param operand the operand
     * */
    public boolean compare(final SimplePredicate.Op op, final String valInTuple, final String operand) {

      final int cmpVal = valInTuple.compareTo(operand);

      switch (op) {
        case EQUALS:
          return cmpVal == 0;

        case NOT_EQUALS:
          return cmpVal != 0;

        case GREATER_THAN:
          return cmpVal > 0;

        case GREATER_THAN_OR_EQ:
          return cmpVal >= 0;

        case LESS_THAN:
          return cmpVal < 0;

        case LESS_THAN_OR_EQ:
          return cmpVal <= 0;

        case LIKE:
          return valInTuple.indexOf(operand) >= 0;
      }

      return false;
    }

    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> stringColumn, final int tupleIndex,
        final Object operand) {
      final String string = ((StringColumn) stringColumn).getString(tupleIndex);
      return compare(op, string, (String) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return ((StringColumn) column).getString(tupleIndex);
    }

    @Override
    public Object fromString(final String str) {
      return str;
    }
  },
  /**
   * Long type.
   * */
  LONG_TYPE() {
    /**
     * @return true if valueInTuple op operand.
     * @param op the operation
     * @param valueInTuple the value to be compared in a tuple
     * @param operand the operand
     * */
    public boolean compare(final SimplePredicate.Op op, final long valueInTuple, final long operand) {
      switch (op) {
        case EQUALS:
          return valueInTuple == operand;
        case NOT_EQUALS:
          return valueInTuple != operand;

        case GREATER_THAN:
          return valueInTuple > operand;

        case GREATER_THAN_OR_EQ:
          return valueInTuple >= operand;

        case LESS_THAN:
          return valueInTuple < operand;

        case LESS_THAN_OR_EQ:
          return valueInTuple <= operand;

        case LIKE:
          return valueInTuple == operand;
      }

      return false;
    }

    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> longColumn, final int tupleIndex,
        final Object operand) {
      final long v = ((LongColumn) longColumn).getLong(tupleIndex);
      return compare(op, v, (Long) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + ((LongColumn) column).getLong(tupleIndex);
    }

    @Override
    public Object fromString(final String str) {
      return Long.valueOf(str);
    }
  },

  /**
   * date type.
   * */
  DATETIME_TYPE() {
    /**
     * @return true if valueInTuple op operand.
     * @param op the operation
     * @param valueInTuple the value to be compared in a tuple
     * @param operand the operand
     * */
    public boolean compare(final SimplePredicate.Op op, final DateTime valueInTuple, final DateTime operand) {
      switch (op) {
        case EQUALS:
          return valueInTuple.equals(operand);
        case NOT_EQUALS:
          return !valueInTuple.equals(operand);
        case GREATER_THAN:
          return valueInTuple.compareTo(operand) > 0;
        case GREATER_THAN_OR_EQ:
          return valueInTuple.compareTo(operand) >= 0;
        case LESS_THAN:
          return valueInTuple.compareTo(operand) < 0;
        case LESS_THAN_OR_EQ:
          return valueInTuple.compareTo(operand) <= 0;
        case LIKE:
          return valueInTuple.equals(operand);
      }

      return false;
    }

    @Override
    public boolean filter(final SimplePredicate.Op op, final Column<?> dateColumn, final int tupleIndex,
        final Object operand) {
      final DateTime v = ((DateTimeColumn) dateColumn).getDateTime(tupleIndex);
      return compare(op, v, (DateTime) operand);
    }

    @Override
    public String toString(final Column<?> column, final int tupleIndex) {
      return "" + ((DateTimeColumn) column).getDateTime(tupleIndex);
    }

    @Override
    public Object fromString(final String str) {
      return DateTime.parse(str);
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
  public Object fromString(final String str) {
    throw new UnsupportedOperationException();
  }
}
