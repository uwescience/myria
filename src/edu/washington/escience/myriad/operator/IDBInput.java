package edu.washington.escience.myriad.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

public class IDBInput extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2;

  private boolean child1Ended = false;
  private boolean emptyDelta = true;

  public IDBInput() {
  }

  public IDBInput(final Operator child1, final Operator child2) {
    // should have some restrictions on which operators are valid, e.g. child1 = scan, child2 = consumer. later
    Preconditions.checkArgument(child1.getSchema().equals(child2.getSchema()));
    this.child1 = child1;
    this.child2 = child2;
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb;
    if (!child1Ended) {
      if ((tb = child1.nextReady()) != null) {
        // not a bug
        emptyDelta &= (tb.numTuples() == 0);
        return tb;
      }
    } else {
      if ((tb = child2.nextReady()) != null) {
        // not a bug
        emptyDelta &= tb.numTuples() == 0;
        return tb;
      }
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tb;
    if ((tb = child1.next()) != null) {
      // not a bug
      emptyDelta &= tb.numTuples() == 0;
      return tb;
    }
    if (!child1Ended) {
      return null;
    }
    if ((tb = child2.next()) != null) {
      // not a bug
      emptyDelta &= tb.numTuples() == 0;
      return tb;
    }
    return null;
  }

  @Override
  public final void checkEOSAndEOI() {
    if (!child1Ended) {
      if (child1.eos()) {
        setEOI(true);
        emptyDelta = true;
        child1Ended = true;
      }
    } else {
      if (child2.eoi()) {
        child2.setEOI(false);
        if (emptyDelta) {
          setEOS();
        } else {
          setEOI(true);
        }
        emptyDelta = true;
      }
    }
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    return child1.getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }

  @Override
  protected void cleanup() throws DbException {
  }

}
