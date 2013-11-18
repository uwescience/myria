package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;

/**
 * Implementation of set difference. Duplicates are not preserved.
 * 
 * @author whitaker
 */
public class Difference extends BinaryOperator {

  /**
   * Instantiate a set difference operator.
   * 
   * @param left the operator being subtracted from.
   * @param right the operator to be subtracted.
   */
  public Difference(final Operator left, final Operator right) {
    super(left, right);
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  protected Schema generateSchema() {
    return getLeft().getSchema();
  }
}
