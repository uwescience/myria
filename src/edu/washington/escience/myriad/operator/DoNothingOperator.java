package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.parallel.DbException;
import edu.washington.escience.myriad.table._TupleBatch;

public class DoNothingOperator extends Operator {

  Operator[] children;
  Schema outputSchema;

  public DoNothingOperator(final Schema outputSchema, final Operator[] children) {
    this.outputSchema = outputSchema;
    this.children = children;
  }

  @Override
  protected final _TupleBatch fetchNext() throws DbException {
    if (children != null) {
      for (final Operator child : children) {
        while (child.hasNext()) {
          child.next();
        }
      }
    }
    return null;
  }

  @Override
  public final Operator[] getChildren() {
    return this.children;
  }

  @Override
  public final Schema getSchema() {
    return this.outputSchema;
  }

  @Override
  public final void open() throws DbException {
    if (children != null) {
      for (final Operator child : children) {
        child.open();
      }
    }
    super.open();
  }

  @Override
  public final void setChildren(final Operator[] children) {
    this.children = children;
  }

}