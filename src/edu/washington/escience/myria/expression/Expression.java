package edu.washington.escience.myria.expression;

import java.util.List;

import edu.washington.escience.myria.TupleBatch;

/**
 * 
 * @author dominik
 * 
 */
public class Expression {
  private String outputName;
  private String javaExpression;
  List<Variable> indexes;

  public Object eval(TupleBatch tb, int rowId) {
    return null;
  }
}
