package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Represents a reference to the type of a child field in an expression tree.
 */
public class TypeOfExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /** The index in the input that is referenced. */
  @JsonProperty private final int columnIdx;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private TypeOfExpression() {
    this(-1);
  }

  /**
   * A {@link TypeOfExpression} that references the type of column <code>columnIdx</code> from the input.
   *
   * @param columnIdx the index in the input.
   */
  public TypeOfExpression(final int columnIdx) {
    this.columnIdx = columnIdx;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return parameters.getSchema().getColumnType(columnIdx);
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    throw new UnsupportedOperationException(
        "This expression operator does not have a java string representation.");
  }

  /**
   * @return the column index of this variable.
   */
  public int getColumnIdx() {
    return columnIdx;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName(), columnIdx);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof TypeOfExpression)) {
      return false;
    }
    TypeOfExpression otherExp = (TypeOfExpression) other;
    return Objects.equals(columnIdx, otherExp.columnIdx);
  }
}
