package edu.washington.escience.myria.expression;

import com.google.common.base.Preconditions;

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
   * Constructor without schema.
   * 
   * @param expression the expression for the evaluator
   */
  public Evaluator(final Expression expression) {
    this.expression = expression;
  }

  /**
   * Default constructor.
   * 
   * @param expression the expression for the evaluator
   * @param schema the schema that the expression expects if it operates on a schema
   */
  public Evaluator(final Expression expression, final Schema schema) {
    Preconditions.checkNotNull(schema);
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
