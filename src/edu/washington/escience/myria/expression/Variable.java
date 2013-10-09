/**
 * 
 */
package edu.washington.escience.myria.expression;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Type;

/**
 * @author dominik
 * 
 */
public class Variable extends ExpressionOperator {

  private final int columnIdx;

  public Variable(int columnIdx) {
    this.columnIdx = columnIdx;
  }

  @Override
  public Set<Variable> getVariables() {
    return ImmutableSet.of(this);
  }

  @Override
  public Type getOutputType() {
    // TODO: get type from schema
    return null;
  }

  @Override
  public String getJavaString() {
    return "col" + columnIdx;
  }

}
