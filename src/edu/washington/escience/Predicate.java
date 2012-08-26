package edu.washington.escience;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable {

  /** Constants used for return codes in Field.compare */
  public enum Op implements Serializable {
    EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

    /**
     * Interface to access operations by integer value for command-line convenience.
     * 
     * @param i a valid integer Op index
     */
    public static Op getOp(int i) {
      return values()[i];
    }

    @Override
    public String toString() {
      if (this == EQUALS)
        return "=";
      if (this == GREATER_THAN)
        return ">";
      if (this == LESS_THAN)
        return "<";
      if (this == LESS_THAN_OR_EQ)
        return "<=";
      if (this == GREATER_THAN_OR_EQ)
        return ">=";
      if (this == LIKE)
        return "LIKE";
      if (this == NOT_EQUALS)
        return "<>";
      throw new IllegalStateException("impossible to reach here");
    }

  }

  private static final long serialVersionUID = 1L;
  private final Op op;
  private final int field;

  private final Field operand;

  /**
   * Constructor.
   * 
   * @param field field number of passed in s to compare against.
   * @param op operation to use for comparison
   * @param operand field value to compare passed in s to
   */
  public Predicate(int field, Op op, Field operand) {
    this.field = field;
    this.op = op;
    this.operand = operand;
  }

  /**
   * @return the field number
   */
  public int getField() {
    return this.field;
  }

  /**
   * @return the operator
   */
  public Op getOp() {
    return this.op;
  }

  /**
   * @return the operand
   */
  public Field getOperand() {
    return this.operand;
  }

  // /**
  // * Compares the field number of t specified in the constructor to the
  // * operand field specified in the constructor using the operator specific in
  // * the constructor. The comparison can be made through Field's compare
  // * method.
  // *
  // * @param t
  // * The tuple to compare against
  // * @return true if the comparison is true, false otherwise.
  // */
  // public boolean filter(Tuple t) {
  // Field f = t.getField(field);
  // return f.compare(op, operand);
  // }

  /**
   * Returns something useful, like "f = field_id op = op_string operand = operand_string
   */
  @Override
  public String toString() {
    String p = "";
    p += "f = " + field + " op = " + op + " operand = " + operand;
    return p;
  }
}
