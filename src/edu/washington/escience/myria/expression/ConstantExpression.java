package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * An expression that returns a constant value.
 */
public class ConstantExpression extends ZeroaryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /** The type of this object. */
  private final Type type;
  /** The value of this object. */
  private final String value;

  /**
   * @param type the type of this object.
   * @param value the value of this object.
   */
  @JsonCreator
  public ConstantExpression(@JsonProperty("value_type") final Type type, @JsonProperty("value") final String value) {
    this.type = type;
    this.value = value;
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return type;
  }

  @Override
  public String getJavaString(final Schema schema) {
    return value;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof ConstantExpression)) {
      return false;
    }
    ConstantExpression otherExp = (ConstantExpression) other;
    return Objects.equals(type, otherExp.type) && Objects.equals(value, otherExp.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
