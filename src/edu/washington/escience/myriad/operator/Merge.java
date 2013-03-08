package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

public final class Merge extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2;
  private final Schema outputSchema;

  public Merge(final Schema outputSchema, final Operator child1, final Operator child2) {
    this.outputSchema = outputSchema;
    this.child1 = child1;
    this.child2 = child2;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch tb;
    if (child1 != null && (tb = child1.next()) != null) {
      return tb;
    }
    if (child2 != null) {
      if ((tb = child2.next()) != null) {
        return tb;
      }
    }
    return null;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return null;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }

}
