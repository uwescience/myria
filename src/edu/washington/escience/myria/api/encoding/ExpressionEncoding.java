package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;

/**
 * Holds the AST and output name
 */
public class ExpressionEncoding extends MyriaApiEncoding {
  public String outputName;
  public ExpressionOperator rootExpressionOperator;

  private static final ImmutableList<String> requiredFields = ImmutableList.of("outputName", "rootExpressionOperator");

  @Override
  protected List<String> getRequiredFields() {
    return requiredFields;
  }

  public Expression construct() {
    // TODO: build expression object
    return null;
  }
}
