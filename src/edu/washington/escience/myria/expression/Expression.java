package edu.washington.escience.myria.expression;

import java.util.List;

import edu.washington.escience.myria.TupleBatch;

/**
 * 
 * @author dominik
 * 
 */
public class Expression {
  /**
   * Name of the column that the result will be written to.
   */
  private final String outputName;

  /**
   * The java expression to be evaluated in {@link #eval}.
   */
  private final String javaExpression;

  /**
   * List of variables used in {@link #javaExpression}.
   */
  private final List<Variable> indexes;

  /**
   * Constructs the Expression object.
   * 
   * @param outputName the name of the resulting element
   * @param javaExpression the expression to be evaluated
   * @param indexes variables that are used in the javaExpression
   */
  public Expression(final String outputName, final String javaExpression, final List<Variable> indexes) {
    this.outputName = outputName;
    this.javaExpression = javaExpression;
    this.indexes = indexes;

    compile();
  }

  /**
   * Compiles the {@link #javaExpression}.
   */
  private void compile() {
    // TODO
  }

  /**
   * Evaluates the expression.
   * 
   * @param tb a tuple batch
   * @param rowId the row that should be used for input data
   * @return the result from the evaluation
   */
  public Object eval(final TupleBatch tb, final int rowId) {
    return null;
  }

  /**
   * @return the output name
   */
  public String getOutputName() {
    return outputName;
  }
}
