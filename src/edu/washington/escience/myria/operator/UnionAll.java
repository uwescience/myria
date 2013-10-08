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
    super(children);
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    for (int i = 0; i < childrenWithData.size(); i++) {
      Integer childIdx = itr.next();
      if (!itr.hasNext()) {
        // simulate circular linked list
        itr = childrenWithData.iterator();
      }

      Operator child = getChild(childIdx);
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
    Objects.requireNonNull(getChildren());
    Preconditions.checkArgument(getNumChildren() > 0);

    childrenWithData = new LinkedList<Integer>();
    int i = 0;
    for (Operator child : getChildren()) {
      Preconditions.checkNotNull(child);
      Preconditions.checkArgument(getSchema().equals(child.getSchema()));
      childrenWithData.add(i++);
    }
    itr = childrenWithData.iterator();
  }
}
