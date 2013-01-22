package edu.washington.escience.myriad.operator;

import java.io.Serializable;
import java.util.Arrays;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * Abstract class for implementing operators.
 * 
 * @author slxu
 * 
 *         Currently, the operator api design requires that each single operator instance should be executed within a
 *         single thread.
 * 
 *         No multi-thread synchronization is considered.
 * 
 */
public abstract class Operator implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * A single buffer for temporally holding a TupleBatch for pull.
   * */
  private TupleBatch outputBuffer = null;

  /**
   * A bit denoting whether the operator is open (initialized).
   * */
  private boolean open = false;

  /**
   * EOS. Initially set it as true;
   * */
  private boolean eos = true;

  private boolean eoi = true;

  /**
   * Do the clean up, release resources.
   * 
   * @throws DbException if any error occurs
   * */
  protected abstract void cleanup() throws DbException;

  /**
   * Closes this iterator.
   * 
   * @throws DbException if any errors occur
   */
  public final void close() throws DbException {
    // Ensures that a future call to next() will fail
    outputBuffer = null;
    open = false;
    setEOS(true);
    setEOI(true);
    cleanup();
    final Operator[] children = getChildren();
    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          child.close();
        }
      }
    }
  }

  /**
   * Check if EOS is meet.
   * 
   * This method is non-blocking.
   * 
   * @return if the Operator is EOS
   * 
   * */
  public final boolean eos() {
    return eos;
  }

  /**
   * Returns the next output TupleBatch, or null if EOS is meet.
   * 
   * This method is blocking.
   * 
   * 
   * @return the next output TupleBatch, or null if EOS
   * 
   * @throws DbException if any processing error occurs
   * 
   */
  protected abstract TupleBatch fetchNext() throws DbException;

  /**
   * Generate next output TupleBatch if possible. Return null immediately if currently no output can be generated.
   * 
   * Do not block the execution thread in this method, including sleep, wait on locks, etc.
   * 
   * @throws DbException if any error occurs
   * 
   * @return next ready output TupleBatch. null if either EOS or no output TupleBatch can be generated currently.
   * */
  protected abstract TupleBatch fetchNextReady() throws DbException;

  /**
   * @return return the children Operators of this operator. If there is only one child, return an array of only one
   *         element. For join operators, the order of the children is not important. But they should be consistent
   *         among multiple calls.
   */
  public abstract Operator[] getChildren();

  /**
   * @return return the Schema of the output tuples of this operator.
   * 
   */
  public abstract Schema getSchema();

  /**
   * Do the initialization of this operator.
   * 
   * @throws DbException if any error occurs
   * 
   */
  protected abstract void init() throws DbException;

  /**
   * @return true if this operator is open.
   */
  public final boolean isOpen() {
    return open;
  }

  public final boolean eoi() {
    return eoi;
  }

  /**
   * Get next TupleBatch. If EOS has not meet, it will wait until a TupleBatch is ready
   * 
   * This method is blocking.
   * 
   * @throws DbException if there's any problem in fetching the next TupleBatch.
   * 
   * @throws IllegalStateException if the operator is not open yet
   * 
   * @return next TupleBatch
   * */
  public final TupleBatch next() throws DbException {
    if (!open) {
      throw new IllegalStateException("Operator not yet open");
    }
    if (eos() || eoi()) {
      return null;
    }

    TupleBatch result = null;
    if (outputBuffer != null) {
      result = outputBuffer;
    } else {
      result = fetchNext();
    }
    outputBuffer = null;

    while (result != null && result.numTuples() <= 0) {
      result = fetchNext();
    }
    if (result == null) {
      checkEOSAndEOI();
    }
    return result;
  }

  public void checkEOSAndEOI() {
    Operator[] children = getChildren();
    boolean[] childrenEOI = getChildrenEOI();
    boolean allEOS = true;
    int count = 0;
    for (int i = 0; i < children.length; ++i) {
      if (children[i].eos()) {
        childrenEOI[i] = true;
      } else if (children[i].eoi()) {
        childrenEOI[i] = true;
        children[i].setEOI(false);
        allEOS = false;
      }
      if (childrenEOI[i]) {
        count++;
      }
    }
    if (count == children.length) {
      if (allEOS) {
        setEOS(true);
      } else {
        setEOI(true);
      }
      cleanChildrenEOI();
    }
  }

  /**
   * Check if currently there's any TupleBatch available for pull.
   * 
   * This method is non-blocking.
   * 
   * @throws DbException if any problem
   * 
   * @return if currently there's output for pulling.
   * 
   * */
  public final boolean nextReady() throws DbException {
    if (!open) {
      throw new DbException("Operator not yet open");
    }
    if (eos()) {
      throw new DbException("Operator already eos");
    }

    if (outputBuffer == null) {
      outputBuffer = fetchNextReady();
      while (outputBuffer != null && outputBuffer.numTuples() <= 0) {
        // XXX while or not while? For a single thread operator, while sounds more efficient generally
        outputBuffer = fetchNextReady();
      }
    }

    return outputBuffer == null;
  }

  /**
   * open the operator and do initializations.
   * 
   * @throws DbException if any error occurs
   * */
  public final void open() throws DbException {
    // open the children first
    if (open) {
      // XXX Do some error handling to multi-open?
      throw new DbException("Operator already open.");
    }
    final Operator[] children = getChildren();
    if (children != null) {
      for (final Operator child : children) {
        if (child != null) {
          child.open();
        }
      }
    }
    setEOS(false);
    setEOI(false);
    // do my initialization
    init();
    open = true;
  }

  /**
   * Explicitly set EOS for this operator.
   * 
   * Only call this method if the operator is a leaf operator.
   * 
   * */
  public final void setEOS(boolean x) {
    eos = x;
  }

  public final void setEOI(boolean x) {
    eoi = x;
  }

  /**
   * Returns the next output TupleBatch, or null if EOS is meet.
   * 
   * This method is blocking.
   * 
   * 
   * @return the next output TupleBatch, or null if EOS Set the children(child) of this operator. If the operator has
   *         only one child, children[0] should be used. If the operator is a join, children[0] and children[1] should
   *         be used.
   * 
   * 
   * @param children the Operators which are to be set as the children(child) of this operator
   */
  // have we ever used this function?
  public abstract void setChildren(Operator[] children);

  public boolean[] childrenEOI = null;

  public boolean[] getChildrenEOI() {
    if (childrenEOI == null) {
      childrenEOI = new boolean[getChildren().length];
    }
    return childrenEOI;
  }

  public void cleanChildrenEOI() {
    Arrays.fill(childrenEOI, false);
  }
}
