package edu.washington.escience.myria;

import java.io.Serializable;

import com.google.common.base.Preconditions;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class SimplePredicate implements Serializable {

  /** Constants used for return codes in Field.compare. */
  public enum Op implements Serializable {
    /** = or ==. */
    EQUALS,
    /** > . */
    GREATER_THAN,
    /** < . */
    LESS_THAN,
    /** <= . */
    LESS_THAN_OR_EQ,
    /** >= . */
    GREATER_THAN_OR_EQ,
    /** LIKE . */
    LIKE,
    /** <> or !=. */
    NOT_EQUALS;

    /**
     * Interface to access operations by integer value for command-line convenience.
     *
     * @param i a valid integer Op index
     * @return the operator at the specified index.
     */
    public static Op getOp(final int i) {
      Preconditions.checkElementIndex(i, values().length);
      return values()[i];
    }

    @Override
    public String toString() {
      switch (this) {
        case EQUALS:
          return "=";
        case GREATER_THAN:
          return ">";
        case LESS_THAN:
          return "<";
        case GREATER_THAN_OR_EQ:
          return ">=";
        case LESS_THAN_OR_EQ:
          return "<=";
        case LIKE:
          return "LIKE";
        case NOT_EQUALS:
          return "<>";
      }
      throw new UnsupportedOperationException("Don't know how to convert op to string " + name());
    }

    /**
     * @return the representation of this Java string as an infix Java operator.
     */
    public String toJavaString() {
      switch (this) {
        case EQUALS:
          return "==";
        case GREATER_THAN:
          return ">";
        case LESS_THAN:
          return "<";
        case GREATER_THAN_OR_EQ:
          return ">=";
        case LESS_THAN_OR_EQ:
          return "<=";
        case NOT_EQUALS:
          return "!=";
        case LIKE:
          throw new UnsupportedOperationException();
      }
      throw new UnsupportedOperationException("Don't know how to convert op to string " + name());
    }
  }

  /** Required for serialization. */
  private static final long serialVersionUID = 1L;
  /** The logical boolean operator this predicate represents. */
  private final Op op;
  /** Which column of the tuple this predicate tests. */
  private final int columnIndex;
  /** The (often constant) right operand of the operator. E.g., the predicate can be "Is greater than 5". */
  private final String operand;

  /**
   * Constructor.
   *
   * @param field field number of passed in s to compare against.
   * @param op operation to use for comparison
   * @param operand field value to compare passed in s to
   */
  public SimplePredicate(final int field, final Op op, final String operand) {
    columnIndex = field;
    this.op = op;
    this.operand = operand;
  }

  /**
   * @return the field number
   */
  public final int getField() {
    return columnIndex;
  }

  /**
   * @return the operator
   */
  public final Op getOp() {
    return op;
  }

  /**
   * @return the operand
   */
  public final String getOperand() {
    return operand;
  }

  @Override
  public final String toString() {
    String p = "";
    p += "f = " + columnIndex + " op = " + op + " operand = " + operand;
    return p;
  }
}
