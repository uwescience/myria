/**
 * 
 */
package edu.washington.escience.myria.expression;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.Schema;

/**
 * An abstract class representing some variable in an expression tree.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @Type(name = "Constant", value = ConstantExpression.class), @Type(name = "Divide", value = DivideExpression.class),
    @Type(name = "Minus", value = MinusExpression.class), @Type(name = "Negate", value = NegateExpression.class),
    @Type(name = "Plus", value = PlusExpression.class), @Type(name = "Pow", value = PowExpression.class),
    @Type(name = "Sqrt", value = SqrtExpression.class), @Type(name = "Times", value = TimesExpression.class),
    @Type(name = "Variable", value = VariableExpression.class), })
public abstract class ExpressionOperator {
  /**
   * @return the set of all variables used in this expression.
   */
  @JsonIgnore
  public abstract Set<VariableExpression> getVariables();

  /**
   * @param schema the schema of the tuples this expression references.
   * @return the type of the output of this expression.
   */
  @JsonIgnore
  public abstract edu.washington.escience.myria.Type getOutputType(final Schema schema);

  /**
   * @return the entire tree represented as an expression.
   */
  @JsonIgnore
  public abstract String getJavaString();

  @Override
  public final String toString() {
    return getJavaString();
  }
}