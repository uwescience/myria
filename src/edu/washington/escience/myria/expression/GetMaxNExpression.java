package edu.washington.escience.myria.expression;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 * Expression operator that returns the maxN from the consistent hash.
 */
public class GetMaxNExpression extends NAryExpression {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private GetMaxNExpression() {
    super();
  }

  /**
   * @param numMaxWorker the max number of workers in the consistent hash ring
   * @param numReplicas the number of replicas in the consistent hash ring
   * @param curNumWorkers the current number of workers
   * @param hashIndices the indices in which we hash the tuples on
   */
  public GetMaxNExpression(final ExpressionOperator numMaxWorker, final ExpressionOperator numReplicas,
      final ExpressionOperator curNumWorkers, final List<ExpressionOperator> hashIndices) {
    super(new ImmutableList.Builder<ExpressionOperator>().add(numMaxWorker).add(numReplicas).add(curNumWorkers).addAll(
        hashIndices).build());
    List<ExpressionOperator> childrenCheck = getChildren();
    for (int i = 0; i < childrenCheck.size(); i++) {
      if (i == 0 || i == 1 || i == 2) {
        // Check for maxWorker, numReplicas, currentNumWorkers
        Preconditions.checkArgument(childrenCheck.get(i) instanceof ConstantExpression, "Child at index " + i
            + " has to be constant");
      } else {
        // Check the rest for hash indices
        Preconditions.checkArgument(childrenCheck.get(i) instanceof VariableExpression, "Child at index " + i
            + " has to be variable");
      }
    }
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return Type.INT_TYPE;
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    /*
     * We should be calling ConsistentHash object. Here the parameters is the numWorkers.
     * 
     * String: ConsistentHash.getMaxN(maxWorker, numReplicas, hashValue, numWorkers);
     */
    List<ExpressionOperator> children = getChildren();

    /* Generate the java string for getting the hashValue. */
    StringBuilder getHashCodeString =
        new StringBuilder(Expression.TB).append(".hashCode(").append(Expression.ROW).append(", new int[] {");
    for (int i = 3; i < children.size(); i++) {
      getHashCodeString.append(((VariableExpression) children.get(i)).getColumnIdx()).append(",");
    }
    getHashCodeString.deleteCharAt(getHashCodeString.length() - 1); // delete the extra ","
    getHashCodeString.append("})");

    /* Generate the string to return the maxN value. */
    StringBuilder sb = new StringBuilder();
    sb.append("edu.washington.escience.myria.parallel."); // the package name
    sb.append("ConsistentHash.getMaxN("); // the function call
    sb.append(children.get(0).getJavaString(parameters)).append(", "); // the maxWorker
    sb.append(children.get(1).getJavaString(parameters)).append(", "); // the numReplicas
    sb.append(getHashCodeString.toString()).append(", "); // the hash value
    sb.append(children.get(2).getJavaString(parameters)); // the number of current workers
    sb.append(")");
    return sb.toString();
  }
}
