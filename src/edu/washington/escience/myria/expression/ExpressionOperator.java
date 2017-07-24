/**
 *
 */
package edu.washington.escience.myria.expression;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * An abstract class representing some variable in an expression tree.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  /* Zeroary */
  @Type(name = "CONSTANT", value = ConstantExpression.class),
  @Type(name = "RANDOM", value = RandomExpression.class),
  @Type(name = "STATE", value = StateExpression.class),
  @Type(name = "TYPE", value = TypeExpression.class),
  @Type(name = "TYPEOF", value = TypeOfExpression.class),
  @Type(name = "VARIABLE", value = VariableExpression.class),
  @Type(name = "WORKERID", value = WorkerIdExpression.class),
  /* Unary */
  @Type(name = "ABS", value = AbsExpression.class),
  @Type(name = "CAST", value = CastExpression.class),
  @Type(name = "CEIL", value = CeilExpression.class),
  @Type(name = "COS", value = CosExpression.class),
  @Type(name = "DOWNLOADBLOB", value = DownloadBlobExpression.class),
  @Type(name = "FLOOR", value = FloorExpression.class),
  @Type(name = "LEN", value = LenExpression.class),
  @Type(name = "LOG", value = LogExpression.class),
  @Type(name = "MD5", value = HashMd5Expression.class),
  @Type(name = "NEG", value = NegateExpression.class),
  @Type(name = "NGRAM", value = NgramExpression.class),
  @Type(name = "NOT", value = NotExpression.class),
  @Type(name = "SEQUENCE", value = SequenceExpression.class),
  @Type(name = "SIN", value = SinExpression.class),
  @Type(name = "SQRT", value = SqrtExpression.class),
  @Type(name = "TAN", value = TanExpression.class),
  @Type(name = "UPPER", value = ToUpperCaseExpression.class),
  /* Binary */
  @Type(name = "AND", value = AndExpression.class),
  @Type(name = "DIVIDE", value = DivideExpression.class),
  @Type(name = "EQ", value = EqualsExpression.class),
  @Type(name = "GREATER", value = GreaterExpression.class),
  @Type(name = "LIKE", value = LikeExpression.class),
  @Type(name = "GT", value = GreaterThanExpression.class),
  @Type(name = "GTEQ", value = GreaterThanOrEqualsExpression.class),
  @Type(name = "IDIVIDE", value = IntDivideExpression.class),
  @Type(name = "LESSER", value = LesserExpression.class),
  @Type(name = "LTEQ", value = LessThanOrEqualsExpression.class),
  @Type(name = "LT", value = LessThanExpression.class),
  @Type(name = "MINUS", value = MinusExpression.class),
  @Type(name = "MODULO", value = ModuloExpression.class),
  @Type(name = "NEQ", value = NotEqualsExpression.class),
  @Type(name = "OR", value = OrExpression.class),
  @Type(name = "PLUS", value = PlusExpression.class),
  @Type(name = "POW", value = PowExpression.class),
  @Type(name = "SPLIT", value = SplitExpression.class),
  @Type(name = "TIMES", value = TimesExpression.class),
  /* Nary */
  @Type(name = "CONDITION", value = ConditionalExpression.class),
  @Type(name = "SUBSTR", value = SubstrExpression.class),
  @Type(name = "PYUDF", value = PyUDFExpression.class)
})
public abstract class ExpressionOperator implements Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Get the output type of the expression which might depend on the types of the children. Also, check whether the
   * types of the children are correct.
   *
   * @param parameters parameters that are needed to determine the output type
   * @return the type of the output of this expression.
   */
  public abstract edu.washington.escience.myria.Type getOutputType(
      final ExpressionOperatorParameter parameters);

  /**
   * @param parameters parameters that are needed to create the java expression
   * @return the entire tree represented as an expression.
   */
  public abstract String getJavaString(final ExpressionOperatorParameter parameters);

  /**
   * @param parameters parameters that are needed to create the java expression
   * @return Java code to efficiently append results to an output column
   */
  public String getJavaExpressionWithAppend(final ExpressionOperatorParameter parameters) {
    return null;
  }

  /**
   * @return if this expression returns a primitive array
   */
  public boolean hasArrayOutputType() {
    return false;
  }

  /**
   * @return all children
   */
  @Nonnull
  public abstract List<ExpressionOperator> getChildren();
}
