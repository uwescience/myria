package edu.washington.escience.myria.expression;

/**
 * Interface for evaluating janino expressions.
 */
public interface ConstEvalInterface {
  /**
   * The interface for evaluating an expression that creates a constant.
   * 
   * @return the constant result from the evaluation
   */
  Object evaluate();
}
