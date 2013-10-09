package edu.washington.escience.myria.operator;

import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.expression.Expression;

/**
 * 
 * @author dominik
 * 
 */
public class Apply extends UnaryOperator {

  public Apply(Operator child, List<Expression> expressions) {
    super(child);
    // TODO Auto-generated constructor stub
  }

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Schema getSchema() {
    // TODO Auto-generated method stub
    return null;
  }

}
