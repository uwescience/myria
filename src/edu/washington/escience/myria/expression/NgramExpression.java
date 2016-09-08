package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Returns a sequence of substrings of a given string, split on a regular expression.
 */
public class NgramExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private NgramExpression() {
    super();
  }

  /**
   * Takes the string to be split and the regex to split it on.
   *
   * @param left the left operand (index of string column).
   * @param right the right operand (regular expression string).
   */
  public NgramExpression(final ExpressionOperator left, final ExpressionOperator right) {
    super(left, right);
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    checkOperandTypes(Type.STRING_TYPE, Type.LONG_TYPE, parameters);
    return Type.STRING_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return new StringBuilder()
        .append("String val = (")
        .append(getLeft().getJavaString(parameters))
        .append(");\n")
        .append("int n = (int) (")
        .append(getRight().getJavaString(parameters))
        .append(");\n")
        .append("int numNgrams = val.length() - n + 1;\n")
        .append("String[] ngrams = new String[numNgrams];\n")
        .append("for (int i = 0; i < numNgrams; ++i) {\n")
        .append("ngrams[i] = val.substring(i, i + n));\n")
        .append("}\n")
        .append("return ngrams;\n")
        .toString();
  }

  @Override
  public String getJavaExpressionWithAppend(final ExpressionOperatorParameter parameters) {
    return new StringBuilder()
        .append("String val = (")
        .append(getLeft().getJavaString(parameters))
        .append(");\n")
        .append("int n = (int) (")
        .append(getRight().getJavaString(parameters))
        .append(");\n")
        .append("int numNgrams = val.length() - n + 1;\n")
        .append(Expression.COUNT)
        .append(".appendInt(numNgrams);\n")
        .append("for (int i = 0; i < numNgrams; ++i) {\n")
        .append(Expression.RESULT)
        .append(".appendString(val.substring(i, i + n));\n")
        .append("}\n")
        .toString();
  }

  @Override
  public boolean hasArrayOutputType() {
    return true;
  }
}
