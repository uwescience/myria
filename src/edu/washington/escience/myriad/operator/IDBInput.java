package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

public class IDBInput extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  private Operator child1, child2;
  private Schema outputSchema;

  public IDBInput() {
  }

  public IDBInput(final Schema outputSchema, final Operator child1, final Operator child2) {
    // should have some restrictions on which operators are valid, e.g. child1 = scan, child2 = dupelim. later
    this.outputSchema = outputSchema;
    this.child1 = child1;
    this.child2 = child2;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    TupleBatch tb;
    if (child1 != null && (tb = child1.next()) != null) {
      tuplesSentSinceLastEOI += tb.numTuples();
      return tb;
    }
    if (child2 != null) {
      if ((tb = child2.next()) != null) {
        tuplesSentSinceLastEOI += tb.numTuples();
        return tb;
      }
    }
    return null;
  }

  private boolean child1Ended = false;
  private int tuplesSentSinceLastEOI = 0;

  @Override
  public final void checkEOSAndEOI() {
    if (!child1Ended && child1.eos()) {
      setEOI(true);
      child1Ended = true;
    } else {
      if (child2.eos()) {
        setEOS();
      } else if (child2.eoi()) {
        child2.setEOI(false);
        if (tuplesSentSinceLastEOI == 0) {
          setEOS();
        } else {
          setEOI(true);
          tuplesSentSinceLastEOI = 0;
        }
      }
    }
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }

  @Override
  protected void cleanup() throws DbException {
  }

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    return null;
  }

}
