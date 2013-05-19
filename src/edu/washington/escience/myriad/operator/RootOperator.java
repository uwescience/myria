package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * An abstract class used to make those specialized operators that only consume tuples simpler to implement.
 * 
 * @author dhalperi
 * 
 */
public abstract class RootOperator extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** Source of the tuples to be consumed. */
  private Operator child;

  /**
   * Sets important parameters for successful operation.
   * 
   * @param child the source of tuples that this Root operator consumes.
   */
  public RootOperator(final Operator child) {
    this.child = child;
  }

  /**
   * Perform the function of this operator on the provided tuples. For instance, may print the tuples to the screen or
   * write them to disk.
   * 
   * @param tuples the tuples to be consumed.
   * @throws DbException if there's an error in the database.
   */
  protected abstract void consumeTuples(TupleBatch tuples) throws DbException;

  /**
   * If the child EOS is meet, the method is called back to let the root operators deal with this event.
   * 
   * @throws DbException if any error occurs.
   * */
  protected abstract void childEOS() throws DbException;

  /**
   * call if the child meets EOI.
   * 
   * @throws DbException if any error occurs.
   * */
  protected abstract void childEOI() throws DbException;

  @Override
  protected final TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch tup = null;
    while (!child.eos()) {
      while ((tup = child.next()) != null) {
        consumeTuples(tup);
      }
      if (child.eos()) {
        childEOS();
      } else if (child.eoi()) {
        childEOI();
        child.setEOI(false);
      }
    }
    return null;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    tb = child.nextReady();
    if (tb != null) {
      consumeTuples(tb);
    } else if (child.eoi()) {
      childEOI();
    } else if (child.eos()) {
      childEOS();
    }
    return tb;
  }

  /**
   * @return the source of the tuples that this Root operator consumes.
   */
  public final Operator getChild() {
    return child;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public final void setChildren(final Operator[] children) {
    if (children.length != 1) {
      throw new IllegalArgumentException("a root operator must have exactly one child");
    }
    child = children[0];
  }

  @Override
  public final Schema getSchema() {
    return child.getSchema();
  }

  /**
   * process EOS and EOI logic.
   * */
  @Override
  protected final void checkEOSAndEOI() {
    if (child.eos()) {
      setEOS();
    } else if (child.eoi()) {
      setEOI(true);
      child.setEOI(false);
    }
  }
}
