package edu.washington.escience.myriad;

import java.io.Serializable;

/**
 * Class representing a type in SimpleDB. Types are static objects defined by this class; hence, the
 * Type constructor is private.
 */
public enum Type implements Serializable {
  INT_TYPE() {
    public boolean compare(Predicate.Op op, int valueInTuple, int operand) {
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
    public boolean filter(Predicate.Op op, Column intColumn, int tupleIndex, Object operand) {
      int v = ((IntColumn) intColumn).getInt(tupleIndex);
      return this.compare(op, v, (Integer) operand);
    }

    @Override
    public String toString(Column column, int tupleIndex) {
      return "" + ((IntColumn) column).getInt(tupleIndex);
    }

  },
  FLOAT_TYPE() {
    public boolean compare(Predicate.Op op, float valueInTuple, float operand) {
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
    public boolean filter(Predicate.Op op, Column floatColumn, int tupleIndex, Object operand) {
      float v = ((FloatColumn) floatColumn).getFloat(tupleIndex);
      return this.compare(op, v, (Float) operand);
    }

    @Override
    public String toString(Column column, int tupleIndex) {
      return "" + ((FloatColumn) column).getFloat(tupleIndex);
    }

  },
  DOUBLE_TYPE() {
    public boolean compare(Predicate.Op op, double valueInTuple, double operand) {
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
    public boolean filter(Predicate.Op op, Column doubleColumn, int tupleIndex, Object operand) {
      double v = ((DoubleColumn) doubleColumn).getDouble(tupleIndex);
      return this.compare(op, v, (Double) operand);
    }

    @Override
    public String toString(Column column, int tupleIndex) {
      return "" + ((DoubleColumn) column).getDouble(tupleIndex);
    }

  },
  BOOLEAN_TYPE() {
    public boolean compare(Predicate.Op op, boolean valueInTuple, boolean operand) {
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
    public boolean filter(Predicate.Op op, Column booleanColumn, int tupleIndex, Object operand) {
      boolean v = ((BooleanColumn) booleanColumn).getBoolean(tupleIndex);
      return this.compare(op, v, (Boolean) operand);
    }

    @Override
    public String toString(Column column, int tupleIndex) {
      return ((BooleanColumn) column).getBoolean(tupleIndex) + "";
    }
  },
  STRING_TYPE() {
    /**
     * Compare the specified field to the value of this Field. Return semantics are as specified by
     * Field.compare
     * 
     * @throws IllegalCastException if val is not a StringField
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, String valInTuple, String operand) {

      int cmpVal = valInTuple.compareTo(operand);

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
    public boolean filter(Predicate.Op op, Column stringColumn, int tupleIndex, Object operand) {
      String string = ((StringColumn) stringColumn).getString(tupleIndex);
      return this.compare(op, string, (String) operand);
    }

    @Override
    public String toString(Column column, int tupleIndex) {
      return ((StringColumn) column).getString(tupleIndex);
    }
  };

  public abstract boolean filter(Predicate.Op op, Column column, int tupleIndex, Object operand);

  public abstract String toString(Column column, int tupleIndex);
}
