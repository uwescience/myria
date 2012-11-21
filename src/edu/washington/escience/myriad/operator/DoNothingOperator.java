package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

public class DoNothingOperator extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  Operator[] children;
  Schema outputSchema;

  public DoNothingOperator(final Schema outputSchema, final Operator[] children) {
    this.outputSchema = outputSchema;
    this.children = children;
  }

  @Override
  protected final _TupleBatch fetchNext() throws DbException {
    if (children != null) {
      while (!eos()) {
        for (final Operator child : children) {
          while (!child.eos() && child.nextReady()) {
            child.next();
          }
        }
      }
    }
    return null;
  }

  @Override
  public final Operator[] getChildren() {
    return children;
  }

  @Override
  public final Schema getSchema() {
    return outputSchema;
  }

  @Override
  public final void init() throws DbException {
  }

  @Override
  public final void setChildren(final Operator[] children) {
    this.children = children;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    // TODO Auto-generated method stub
    return null;
  }

}