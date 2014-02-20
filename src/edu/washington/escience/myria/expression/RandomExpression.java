package edu.washington.escience.myria.expression;

import java.util.Objects;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Expression operator that returns a random double greater than or equal to 0.0 and less than 1.0.
 */
public class RandomExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor.
   */
  public RandomExpression() {
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    return Type.DOUBLE_TYPE;
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    return "Math.random()";
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName());
  }

  @Override
  public boolean equals(final Object other) {
    return other != null && other instanceof RandomExpression;
  }
}
