package edu.washington.escience.myria.expression;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Represents a reference to a child field in an expression tree.
 */
public class VariableExpression extends ZeroaryExpression {

  /** The index in the input that this {@link VariableExpression} references. */
  private final int columnIdx;

  /**
   * A {@link VariableExpression} that references column <code>columnIdx</code> from the input.
   * 
   * @param columnIdx the index in the input that this {@link VariableExpression} references.
   */
  public VariableExpression(final int columnIdx) {
    this.columnIdx = columnIdx;
  }

  @Override
  public Set<VariableExpression> getVariables() {
    return ImmutableSet.of(this);
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return schema.getColumnType(columnIdx);
  }

  @Override
  public String getJavaString() {
    return "col" + columnIdx;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof VariableExpression)) {
      return false;
    }
    return ((VariableExpression) other).columnIdx == columnIdx;
  }

  @Override
  public int hashCode() {
    return columnIdx;
  }
}
