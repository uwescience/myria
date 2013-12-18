package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;

/**
 * Compile and evaluate expressions.
 */
public abstract class Evaluator {
  /**
   * The expression to be compiled and evaluated.
   */
  private final Expression expression;

  /**
   * Default constructor.
   * 
   * @param expression the expression for the evaluator
   * @param schema the schema that the expression expects
   */
  public Evaluator(final Expression expression, final Schema schema) {
    this.expression = expression;
    getExpression().setSchema(schema);
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
