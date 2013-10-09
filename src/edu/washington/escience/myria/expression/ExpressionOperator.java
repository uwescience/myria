/**
 * 
 */
package edu.washington.escience.myria.expression;

import java.util.Set;

import edu.washington.escience.myria.Type;

/**
 * @author dominik
 * 
 */
public abstract class ExpressionOperator {
  public abstract Set<Variable> getVariables();

  public abstract Type getOutputType();

  public abstract String getJavaString();
}
