package edu.washington.escience.myria.expression;

import java.util.Collections;
import java.util.List;

/**
 * An ExpressionOperator with no children.
 */
public abstract class ZeroaryExpression extends ExpressionOperator {
  /***/
  private static final long serialVersionUID = 1L;

  @Override
  @SuppressWarnings("unchecked")
  public List<ExpressionOperator> getChildren() {
    return Collections.EMPTY_LIST;
  }
}
