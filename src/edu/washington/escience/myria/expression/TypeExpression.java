package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Expression operator that does not have a value but just a type. Can be used with {@link CastExpression}.
 */
public class TypeExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The type of this expression operator.
   */
  private final Type type;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private TypeExpression() {
    super();
    type = null;
  }

  /**
   * Default constructor.
   * 
   * @param type the type of this expression operator
   */
  public TypeExpression(final Type type) {
    this.type = type;
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    return type;
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    throw new UnsupportedOperationException("This expression operator does not have a java string representation.");
  }
}
