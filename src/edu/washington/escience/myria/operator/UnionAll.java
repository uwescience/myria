package edu.washington.escience.myria.operator;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;

/**
 * Unions the output of a set of operators without eliminating duplicates.
 * */
public final class UnionAll extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The union children. It is required that the schemas of all the children are the same.
   * */
  private Operator[] children;

  /**
   * Fairly get data from children.
   * */
  private transient int childIdxToUnion;

  /**
   * @param children the children to be united.
   * */
  public UnionAll(final Operator[] children) {
    this.children = children;
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    for (int unionCount = 0; unionCount < children.length; unionCount++) {
      Operator child = children[childIdxToUnion];
      childIdxToUnion = (childIdxToUnion + 1) % children.length;
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
    return children[0].getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    childIdxToUnion = 0;
  }

  @Override
  public void setChildren(final Operator[] children) {
    Objects.requireNonNull(children);
    Preconditions.checkArgument(children.length > 0);
    for (Operator op : children) {
      Preconditions.checkArgument(op.getSchema().equals(children[0].getSchema()));
    }
    this.children = children;
  }
}
