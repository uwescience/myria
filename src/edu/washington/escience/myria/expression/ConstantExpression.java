package edu.washington.escience.myria.expression;

import java.util.Objects;

import org.apache.commons.lang.StringEscapeUtils;

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
  @JsonProperty
  private final Type valueType;
  /** The value of this object. */
  @JsonProperty
  private final String value;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ConstantExpression() {
    valueType = null;
    value = null;
  }

  /**
   * @param type the type of this object.
   * @param value the value of this object.
   */
  public ConstantExpression(final Type type, final String value) {
    valueType = type;
    this.value = value;
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return valueType;
  }

  @Override
  public String getJavaString(final Schema schema) {
    switch (valueType) {
      case BOOLEAN_TYPE:
      case DOUBLE_TYPE:
      case FLOAT_TYPE:
      case INT_TYPE:
      case LONG_TYPE:
      case OBJ_TYPE:
        return value;
      case DATETIME_TYPE:
        throw new UnsupportedOperationException("using constant value of type DateTime");
      case STRING_TYPE:
        return '\"' + StringEscapeUtils.escapeJava(value) + '\"';
    }
    throw new UnsupportedOperationException("using constant value of type " + valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName(), valueType, value);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof ConstantExpression)) {
      return false;
    }
    ConstantExpression otherExp = (ConstantExpression) other;
    return Objects.equals(valueType, otherExp.valueType) && Objects.equals(value, otherExp.value);
  }
}
