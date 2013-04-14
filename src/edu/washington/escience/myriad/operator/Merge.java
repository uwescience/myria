package edu.washington.escience.myriad.operator;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * Merge the output of a set of operators.
 * */
public final class Merge extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The merge children. It is required that the schemas of all the children are the same.
   * */
  private final Operator[] children;
  /**
   * The result schema.
   * */
  private final Schema outputSchema;

  /**
   * @param children the children for merging.
   * */
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
    for (Operator child : children) {
      TupleBatch tb = child.next();
      if (tb != null) {
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
      TupleBatch tb = child.nextReady();
      if (tb != null) {
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
    int size = this.children.length;
    if (size > children.length) {
      size = children.length;
    }
    for (int i = 0; i < size; i++) {
      this.children[i] = children[i];
    }
  }
}
