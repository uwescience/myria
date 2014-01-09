package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.DbException;

/**
 * Compile and evaluate expressions.
 */
public abstract class Evaluator {
  /**
   * The expression to be compiled and evaluated.
   */
  private final Expression expression;

  /**
   * @param expression the expression for the evaluator
   */
  public Evaluator(final Expression expression) {
    this.expression = expression;
  }

  /**
   * @return the {@link #expression}
   */
  public Expression getExpression() {
    return expression;
  }

  /**
   * Compiles the {@link #javaExpression}.
   * 
   * @throws DbException compilation failed
   */
  public void compile() throws DbException {
  }
}
