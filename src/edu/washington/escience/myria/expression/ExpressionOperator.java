/**
 * 
 */
package edu.washington.escience.myria.expression;

import java.io.Serializable;

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
    @Type(name = "Abs", value = AbsExpression.class), @Type(name = "Ceil", value = CeilExpression.class),
    @Type(name = "Constant", value = ConstantExpression.class), @Type(name = "Cos", value = CosExpression.class),
    @Type(name = "Divide", value = DivideExpression.class), @Type(name = "Floor", value = FloorExpression.class),
    @Type(name = "Log", value = LogExpression.class), @Type(name = "Minus", value = MinusExpression.class),
    @Type(name = "Negate", value = NegateExpression.class), @Type(name = "Plus", value = PlusExpression.class),
    @Type(name = "Pow", value = PowExpression.class), @Type(name = "Sin", value = SinExpression.class),
    @Type(name = "Sqrt", value = SqrtExpression.class), @Type(name = "Tan", value = TanExpression.class),
    @Type(name = "Times", value = TimesExpression.class), @Type(name = "Upper", value = ToUpperCaseExpression.class),
    @Type(name = "Variable", value = VariableExpression.class), })
public abstract class ExpressionOperator implements Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * @param schema the schema of the tuples this expression references.
   * @return the type of the output of this expression.
   */
  @JsonIgnore
  public abstract edu.washington.escience.myria.Type getOutputType(final Schema schema);

  /**
   * @return the entire tree represented as an expression.
   * 
   * @param schema the input schema
   */
  @JsonIgnore
  public abstract String getJavaString(final Schema schema);
}