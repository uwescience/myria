package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;

/**
 * Implementation of set difference. Duplicates are not preserved.
 * 
 * @author whitaker
 */
public class Difference extends BinaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Instantiate a set difference operator.
   * 
   * @param left the operator being subtracted from.
   * @param right the operator to be subtracted.
   */
  public Difference(final Operator left, final Operator right) {
    super(left, right);
  }

  private TupleBatch processLeftChildTB(TupleBatch leftTB) {
    // TODO Auto-generated method stub
    return null;
  }

  private void processRightChildTB(TupleBatch rightTB) {

  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    final Operator right = getRight();

    /* Drain the right child. */
    while (!right.eos()) {
      TupleBatch rightTB = right.nextReady();
      if (rightTB == null) {
        if (right.eos()) {
          break;
        }
        return null;
      }
      processRightChildTB(rightTB);
    }

    final Operator left = getLeft();
    while (!left.eos()) {
      TupleBatch leftTB = left.nextReady();
      if (leftTB == null) {
        return null;
      }
      return processLeftChildTB(leftTB);
    }

    return null;
  }

  @Override
  protected Schema generateSchema() {
    return getLeft().getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  protected void cleanup() throws DbException {
  }
}
