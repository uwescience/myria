package edu.washington.escience.myria.expression;

import java.util.Objects;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Expression operator that returns the worker ID.
 */
public class WorkerIdExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * An expressions that returns the worker (or master) id.
   */
  public WorkerIdExpression() {}

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return String.valueOf(parameters.getWorkerId());
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return Type.INT_TYPE;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName());
  }

  @Override
  public boolean equals(final Object other) {
    return (other != null && WorkerIdExpression.class.equals(other.getClass()));
  }
}
