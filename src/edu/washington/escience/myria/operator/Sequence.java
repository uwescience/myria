package edu.washington.escience.myria.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Runs each of its children to completion, in order. The output of all children but the last is ignored, so in the
 * common case those children will be {@link RootOperator}s. Returns the output of the last child.
 */
public class Sequence extends NAryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Create a sequence operator wrapping the specified children.
   * 
   * @param children the children.
   */
  public Sequence(final Operator[] children) {
    super(children);
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    Operator[] children = getChildren();
    for (int i = 0; i < children.length; ++i) {
      Operator child = children[i];
      /* Current child has not finished, run it until it's through. */
      while (!child.eos()) {
        TupleBatch tb = child.nextReady();
        /* If this is the last child, return whatever it gives us. */
        if (i == children.length - 1) {
          return tb;
        }
        /* This is not the last child. If this child is done, let's move on to the next child. */
        if (child.eos()) {
          break;
        }
        /*
         * This is not the last child, and it is not done but has no output. Sequence cannot make any more progress, so
         * return null to indicate to the execution environment to check us later.
         */
        if (tb == null) {
          return null;
        }
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   * 
   * Returns the output of the last child.
   */
  @Override
  protected Schema generateSchema() {
    Operator[] children = getChildren();
    if (children == null) {
      return null;
    }

    Preconditions.checkState(children.length > 0, "expecting at least one child");
    Operator c = children[children.length - 1];
    if (c == null) {
      return null;
    }

    return c.getSchema();
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    Operator[] children = getChildren();
    Preconditions.checkNotNull(children, "children");
    Preconditions.checkArgument(children.length > 0, "expected at least one child");
    int i = 0;
    for (Operator child : children) {
      Preconditions.checkNotNull(child, "child " + i);
      ++i;
    }
  }
}
