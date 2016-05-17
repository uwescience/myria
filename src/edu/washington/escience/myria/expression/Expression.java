package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.util.LinkedList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * An expression that can be applied to a tuple.
 */
public class Expression implements Serializable {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Name of the column that the result will be written to.
   */
  @JsonProperty private final String outputName;

  /**
   * The java expression to be evaluated.
   */
  @JsonProperty private String javaExpression;

  /**
   * Expression encoding reference is needed to get the output type.
   */
  @JsonProperty private final ExpressionOperator rootExpressionOperator;

  /**
   * Variable name of result.
   */
  public static final String RESULT = "result";
  /**
   * Variable name of input tuple batch.
   */
  public static final String TB = "tb";
  /**
   * Variable name of row index.
   */
  public static final String ROW = "row";
  /**
   * Variable name of state.
   */
  public static final String STATE = "state";

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  public Expression() {
    outputName = null;
    rootExpressionOperator = null;
  }

  /**
   * Constructs the Expression object.
   *
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public Expression(final ExpressionOperator rootExpressionOperator) {
    this.rootExpressionOperator = rootExpressionOperator;
    outputName = null;
  }

  /**
   * Constructs the Expression object.
   *
   * @param outputName the name of the resulting element
   * @param rootExpressionOperator the root of the AST representing this expression.
   */
  public Expression(final String outputName, final ExpressionOperator rootExpressionOperator) {
    this.rootExpressionOperator = rootExpressionOperator;
    this.outputName = outputName;
  }

  /**
   * @return the rootExpressionOperator
   */
  public ExpressionOperator getRootExpressionOperator() {
    return rootExpressionOperator;
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return outputName;
  }

  /**
   * @param parameters parameters that are needed to create the java expression
   * @return the Java form of this expression.
   */
  public String getJavaExpression(final ExpressionOperatorParameter parameters) {
    if (javaExpression == null) {
      return rootExpressionOperator.getJavaString(parameters);
    }
    return javaExpression;
  }

  /**
   * @param parameters parameters that are needed to create the java expression
   * @return the Java form of this expression that also writes the results to a {@link ColumnBuilder}.
   */
  public String getJavaExpressionWithAppend(final ExpressionOperatorParameter parameters) {
    return new StringBuilder(RESULT)
        .append(".append")
        .append(getOutputType(parameters).getName())
        .append("(")
        .append(getJavaExpression(parameters))
        .append(")")
        .toString();
  }

  /**
   * @param parameters parameters that are needed to determine the output type
   * @return the type of the output
   */
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return rootExpressionOperator.getOutputType(parameters);
  }

  /**
   * Reset {@link #javaExpression}.
   */
  public void resetJavaExpression() {
    javaExpression = null;
  }

  /**
   * @param optype Class to find
   * @return true if the operator is in the expression
   */
  public boolean hasOperator(final Class<?> optype) {
    LinkedList<ExpressionOperator> ops = Lists.newLinkedList();
    ops.add(getRootExpressionOperator());
    while (!ops.isEmpty()) {
      final ExpressionOperator op = ops.pop();
      if (op.getClass().equals(optype)) {
        return true;
      }
      ops.addAll(op.getChildren());
    }
    return false;
  }

  /**
   * An expression is a constant expression when it has to be evaluated only once. This means that an expression with
   * variables, state or random is likely not a constant.
   *
   * @return if this expression evaluates to a constant
   */
  public boolean isConstant() {
    return !hasOperator(VariableExpression.class)
        && !hasOperator(StateExpression.class)
        && !hasOperator(RandomExpression.class);
  }
}
