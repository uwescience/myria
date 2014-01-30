package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Represents a reference to a child field in an expression tree.
 */
public class VariableExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /** The index in the input that is referenced. */
  @JsonProperty
  private final int columnIdx;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private VariableExpression() {
    this(-1);
  }

  /**
   * A {@link VariableExpression} that references column <code>columnIdx</code> from the input.
   * 
   * @param columnIdx the index in the input.
   */
  public VariableExpression(final int columnIdx) {
    this.columnIdx = columnIdx;
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    return schema.getColumnType(columnIdx);
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    // We generate a variable access into the tuple buffer.
    return new StringBuilder(Expression.TB).append(".get").append(getOutputType(schema, stateSchema).getName()).append(
        "(").append(columnIdx).append(", ").append(Expression.ROW).append(")").toString();
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
    if (other == null || !(other instanceof VariableExpression)) {
      return false;
    }
    VariableExpression otherExp = (VariableExpression) other;
    return Objects.equals(columnIdx, otherExp.columnIdx);
  }
}
