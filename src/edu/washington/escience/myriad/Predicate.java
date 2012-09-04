package edu.washington.escience.myriad;

import java.io.Serializable;

import com.google.common.base.Preconditions;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

  /** Constants used for return codes in Field.compare. */
  public enum Op implements Serializable {
    EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

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
          return "==";
        case NOT_EQUALS:
          return "<>";
      }
      throw new IllegalStateException("Shouldn't reach here");
    }

  }

  /** A constant needed for serialization. */
  private static final long serialVersionUID = 1L;
  /** Which operator this Predicate implements. */
  private final Op op;
  /** Which column this Predicate tests. */
  private final int field;
  /** The value this Predicate tests against. E.g., a constant Integer. */
  private final Object operand;

  /**
   * Constructor.
   * 
   * @param field field number of passed in s to compare against.
   * @param op operation to use for comparison
   * @param operand field value to compare passed in s to
   */
  public Predicate(final int field, final Op op, final Object operand) {
    this.field = field;
    this.op = op;
    this.operand = operand;
  }

  /**
   * @return the field number
   */
  public final int getField() {
    return this.field;
  }

  /**
   * @return the operator
   */
  public final Op getOp() {
    return this.op;
  }

  /**
   * @return the operand
   */
  public final Object getOperand() {
    return this.operand;
  }

  @Override
  public final String toString() {
    String p = "";
    p += "f = " + field + " op = " + op + " operand = " + operand;
    return p;
  }
}
