package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.StatefulApply;

/**
 * Simple expression operator that allows access to the state in {@link StatefulApply}.
 */
public class StateExpression extends ConstantExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor.
   */
  public StateExpression() {
    super(Type.OBJ_TYPE, "state");
  }
}
