package edu.washington.escience.myriad.operator;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

public final class Merge extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private final Operator[] children;
  private final Schema outputSchema;

  public Merge(final Operator[] children) {
    Objects.requireNonNull(children);
    Preconditions.checkArgument(children.length > 0);

    for (Operator op : children) {
      Preconditions.checkArgument(op.getSchema().equals(children[0].getSchema()));
    }

    outputSchema = children[0].getSchema();
    this.children = children;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb;
    for (Operator child : children) {
      if ((tb = child.next()) != null) {
        return tb;
      }
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    // boolean allChildrenEOS = true;

    for (Operator child : children) {
      if (child.eos()) {
        continue;
      }
      TupleBatch tb = null;
      if ((tb = child.nextReady()) != null) {
        return tb;
      }
    }

    return null;
  }

  @Override
  public Operator[] getChildren() {
    return children;
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    int size = this.children.length > children.length ? children.length : this.children.length;
    for (int i = 0; i < size; i++) {
      this.children[i] = children[i];
    }
  }
}
