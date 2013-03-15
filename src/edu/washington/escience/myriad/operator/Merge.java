package edu.washington.escience.myriad.operator;

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

  public Merge(final Schema schema, final Operator child1, final Operator child2) {
    Preconditions.checkArgument(child1.getSchema().equals(child2.getSchema()));
    Preconditions.checkArgument(child1.getSchema().equals(schema));
    outputSchema = schema;
    children = new Operator[2];
    children[0] = child1;
    children[1] = child2;
  }

  public Merge(final Operator child1, final Operator child2) {
    Preconditions.checkArgument(child1.getSchema().equals(child2.getSchema()));
    outputSchema = child1.getSchema();
    children = new Operator[2];
    children[0] = child1;
    children[1] = child2;
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
    this.children[0] = children[0];
    this.children[1] = children[1];
  }
}
