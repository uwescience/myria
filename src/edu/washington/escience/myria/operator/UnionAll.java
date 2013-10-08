package edu.washington.escience.myria.operator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.TupleBatch;

/**
 * Unions the output of a set of operators without eliminating duplicates.
 * */
public final class UnionAll extends NAryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * List of children that have not yet returned EOS.
   */
  private transient LinkedList<Integer> childrenWithData;

  /**
   * Iterator into {@link #childrenWithData}. Global to fairly get data from children.
   */
  private transient Iterator<Integer> itr;

  /**
   * @param children the children to be united.
   * */
  public UnionAll(final Operator[] children) {
    if (children != null) {
      setChildren(children);
    } else {
      this.children = null;
    }
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    for (int i = 0; i < childrenWithData.size(); i++) {
      Integer childId = itr.next();
      if (!itr.hasNext()) {
        // simulate circular linked list
        itr = childrenWithData.iterator();
      }

      Operator child = children[childId];
      if (child.eos()) {
        itr.remove();
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
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    Objects.requireNonNull(children);
    childrenWithData = new LinkedList<Integer>();
    int i = 0;
    for (Operator op : children) {
      Preconditions.checkArgument(op.getSchema().equals(children[0].getSchema()));
      childrenWithData.add(i++);
    }
    itr = childrenWithData.iterator();
    super.setChildren(children);
  }
}
