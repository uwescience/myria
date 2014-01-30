package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

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
  @JsonProperty
  private final Type outputType;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private TypeExpression() {
    super();
    outputType = null;
  }

  /**
   * Default constructor.
   * 
   * @param type the type of this expression operator
   */
  public TypeExpression(final Type type) {
    outputType = type;
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    return outputType;
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    throw new UnsupportedOperationException("This expression operator does not have a java string representation.");
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName(), outputType);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof TypeExpression)) {
      return false;
    }
    TypeExpression otherExp = (TypeExpression) other;
    return Objects.equals(outputType, otherExp.outputType);
  }
}
