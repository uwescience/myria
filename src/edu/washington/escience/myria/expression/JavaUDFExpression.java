package edu.washington.escience.myria.expression;

import java.util.List;
import java.util.Iterator;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 *
 * Java UDF expressions.
 *
 */
public class JavaUDFExpression extends NAryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  @JsonProperty private String name = "";

  @JsonProperty private Type outputType = null;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private JavaUDFExpression() {}
  /**
   * Java function expression.
   *
   * @param children operand.
   * @param name - name of the java function in the catalog
   */
  public JavaUDFExpression(
      final List<ExpressionOperator> children, final String name, final Type outputType) {
    super(children);
    this.name = name;
    this.outputType = outputType;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return outputType;
  }

  /**
   * Get this.name(args)
   */
  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    StringBuilder javaString = new StringBuilder(name + "(");

    Iterator<ExpressionOperator> it = getChildren().iterator();
    if (it.hasNext()) {
      javaString.append(it.next().getJavaString(parameters));
    }
    while (it.hasNext()) {
      javaString.append(", ");
      javaString.append(it.next().getJavaString(parameters));
    }

    javaString.append(")");
    return javaString.toString();
  }
}
