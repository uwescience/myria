package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.SimplePredicate.Op;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Comparison in expression tree.
 */
public abstract class ComparisonExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The operation the operation that this comparison expression implements.
   */
  private final SimplePredicate.Op operation;

  /**
   * Returns the operation.
   *
   * @return the operation for this comparison expression
   */
  private SimplePredicate.Op getOperation() {
    Preconditions.checkNotNull(operation);
    return operation;
  }

  /**
   * True if left {@link #getOperation()} right.
   *
   * @param left the left operand.
   * @param right the right operand.
   * @param operation the operation that this comparison expression uses.
   */
  public ComparisonExpression(
      final ExpressionOperator left,
      final ExpressionOperator right,
      final SimplePredicate.Op operation) {
    super(left, right);
    this.operation = operation;
  }

  /**
   * @param operation the operation for this comparison expression.
   */
  public ComparisonExpression(final Op operation) {
    this.operation = operation;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    Type leftType = getLeft().getOutputType(parameters);
    Type rightType = getRight().getOutputType(parameters);

    if (leftType == Type.STRING_TYPE || leftType == Type.DATETIME_TYPE) {
      Preconditions.checkArgument(
          rightType == leftType,
          "If the type of the left child is %s, %s requires right child [%s] of Type %s to be %s as well.",
          leftType,
          getClass().getSimpleName(),
          getRight(),
          rightType,
          leftType);
    } else {
      checkAndReturnDefaultNumericType(parameters);
    }
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    if (getLeft().getOutputType(parameters) == Type.STRING_TYPE) {
      return getObjectComparisonString(getOperation(), parameters);
    }
    return getInfixBinaryString(getOperation().toJavaString(), parameters);
  }
}
