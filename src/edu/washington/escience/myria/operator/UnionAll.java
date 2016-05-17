package edu.washington.escience.myria.operator;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Unions the output of a set of operators without eliminating duplicates.
 */
public final class UnionAll extends NAryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The column names from the first child, whose names we pass on. */
  private List<String> outputColumnNames = null;

  /**
   * List of children that have not yet returned EOS.
   */
  private transient LinkedList<Operator> childrenWithData;

  /**
   * @param children the children to be united.
   */
  public UnionAll(final Operator[] children) {
    super(children);
  }

  @Override
  protected void cleanup() throws DbException {}

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    /*
     * If this variable gets to 0, it means that we've checked every child that could have data and none currently do.
     * At that point, we return null and sleep until one of those children gets data.
     */
    int uncheckedChildren = childrenWithData.size();

    while (!childrenWithData.isEmpty()) {
      if (uncheckedChildren == 0) {
        return null;
      }
      uncheckedChildren--;
      Operator child = childrenWithData.removeFirst();

      if (child.eos()) {
        continue;
      }

      TupleBatch tb = child.nextReady();
      if (!child.eos()) {
        childrenWithData.addLast(child);
      }

      if (tb != null) {
        if (tb.getSchema().getColumnNames().equals(outputColumnNames)) {
          return tb;
        } else {
          return tb.rename(outputColumnNames);
        }
      }
    }

    return null;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator[] children = Objects.requireNonNull(getChildren(), "children");
    Preconditions.checkArgument(children.length > 0, "UnionAll requires at least one child");
    outputColumnNames = children[0].getSchema().getColumnNames();

    childrenWithData = new LinkedList<Operator>();
    for (Operator child : getChildren()) {
      Preconditions.checkNotNull(child, "child");

      Preconditions.checkArgument(
          getSchema().compatible(child.getSchema()),
          "Child schema %s is incompatible with %s",
          child.getSchema(),
          getSchema());
      childrenWithData.add(child);
    }
  }

  @Override
  public Schema generateSchema() {
    Operator[] children = getChildren();
    if (children == null) {
      return null;
    }
    if (children[0] == null) {
      return null;
    }
    return children[0].getSchema();
  }
}
