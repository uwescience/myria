package edu.washington.escience.myria.expression;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

/**
 *
 * @author dominik
 *
 */
public abstract class NAryExpression extends ExpressionOperator {

  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The children of this operator expression.
   */
  @JsonProperty private final List<ExpressionOperator> children;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  protected NAryExpression() {
    children = null;
  }

  /**
   * @param children the children.
   */
  protected NAryExpression(final List<ExpressionOperator> children) {
    this.children = ImmutableList.copyOf(children);
  }

  @Override
  public List<ExpressionOperator> getChildren() {
    return children;
  }

  /**
   * Returns the function call string: child + functionName. E.g, for {@code substring(int beginIndex, int endIndex)},
   * <code>functionName</code> is <code>".substring</code>.
   *
   * @param functionName the string representation of the Java function name.
   * @param parameters parameters that are needed to determine the java string
   * @return the Java string for this operator.
   */
  protected final String getDotFunctionCallString(
      final String functionName, final ExpressionOperatorParameter parameters) {
    StringBuilder callString =
        new StringBuilder(children.get(0).getJavaString(parameters))
            .append(functionName)
            .append("(");
    Iterator<ExpressionOperator> it = children.iterator();
    it.next(); // skip first child because it is what we call
    if (it.hasNext()) {
      callString.append(it.next().getJavaString(parameters));
      while (it.hasNext()) {
        callString.append(",");
        callString.append(it.next().getJavaString(parameters));
      }
    }
    callString.append(")");
    return callString.toString();
  }

  /**
   * A function that could be used as the default type checker for an expression where all operands must be numeric or
   * have the same type.
   *
   * @param parameters parameters that are needed to determine the output type
   * @return the default numeric type, based on the types of the children and Java type precedence.
   */
  protected Type checkAndReturnDefaultType(final ExpressionOperatorParameter parameters) {
    List<Type> types = Lists.newArrayListWithCapacity(getChildren().size());
    for (ExpressionOperator child : getChildren()) {
      types.add(child.getOutputType(parameters));
    }

    // if all types are the same, we can just return it
    if (ImmutableSet.copyOf(types).size() == 1) {
      return types.get(0);
    }

    // otherwise all types have to be numeric and we look for the dominating type
    ImmutableList<Type> validTypes =
        ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.LONG_TYPE, Type.INT_TYPE);
    List<Integer> indexes = Lists.newArrayListWithCapacity(getChildren().size());
    int childIdx = 0;
    for (Type type : types) {
      final int idx = validTypes.indexOf(type);
      indexes.add(idx);
      Preconditions.checkArgument(
          idx != -1,
          "%s cannot handle child [%s] of Type %s",
          getClass().getSimpleName(),
          getChild(childIdx),
          type);
      childIdx++;
    }
    return validTypes.get(Collections.min(indexes));
  }

  /**
   * @param index the child to return
   * @return Child at the index'th position
   */
  protected ExpressionOperator getChild(final int index) {
    return getChildren().get(index);
  }

  /**
   * A function that could be used as the default hash code for an n-ary expression.
   *
   * @return a hash of (getClass().getCanonicalName(), children).
   */
  protected final int defaultHashCode() {
    return Objects.hash(getClass().getCanonicalName(), children);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }
    NAryExpression otherExpr = (NAryExpression) other;
    for (int i = 0; i < children.size(); i++) {
      if (!Objects.equals(getChild(i), otherExpr.getChild(i))) {
        return false;
      }
    }
    return true;
  }
}
