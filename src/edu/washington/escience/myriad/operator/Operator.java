package edu.washington.escience.myriad.operator;

import java.io.Serializable;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Abstract class for implementing operators.
 * 
 * 
 */
public abstract class Operator implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * A single buffer for temporally holding a TupleBatch for pull.
   * */
  private _TupleBatch outputBuffer = null;

  /**
   * A bit denoting whether the operator is open (initialized).
   * */
  private boolean open = false;

  /**
   * EOS.
   * */
  private boolean eos = false;

  /**
   * Closes this iterator.
   * 
   * @throws DbException
   */
  public final void close() throws DbException {
    // Ensures that a future call to next() will fail
    outputBuffer = null;
    open = false;
    eos = false;
    cleanup();
    Operator[] children = getChildren();
    if (children != null) {
      for (Operator child : children) {
        if (child != null) {
          child.close();
        }
      }
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
      throw new IllegalStateException("Operator not yet open");
    }
    if (eos()) {
      throw new IllegalStateException("Operator already eos");
    }

    if (outputBuffer == null) {
      outputBuffer = fetchNextReady();
      if (outputBuffer != null && outputBuffer.numInputTuples() <= 0) {
        outputBuffer = null;
      }
    }

    return outputBuffer == null;
  }

  /**
   * Check if EOS is meet.
   * 
   * This method is non-blocking.
   * 
   * */
  public final boolean eos() {
    // if (eos) {
    // return true;
    // }
    //
    // Operator[] children = getChildren();
    // if (children != null && children.length > 0) {
    // for (Operator child : children) {
    // // if the operator has children, EOS if and only if the children are all EOS
    // if (child != null && !child.eos()) {
    // return false;
    // }
    // }
    // eos = true;
    // }
    return eos;
  }

  /**
   * Get next TupleBatch. If EOS has not meet, it will wait until a TupleBatch is ready
   * 
   * This method is blocking.
   * 
   * @throws DbException if there's any problem in fetching the next TupleBatch.
   * 
   * @throws IllegalStateException if the operator is not open yet
   * */
  public final _TupleBatch next() throws DbException {
    if (!open) {
      throw new IllegalStateException("Operator not yet open");
    }
    if (eos()) {
      return null;
    }

    _TupleBatch result = outputBuffer;
    outputBuffer = null;
    while (result != null && result.numOutputTuples() <= 0) {
      result = fetchNext();
    }

    if (result == null) {
      setEOS();
    }

    return result;
  }

  /**
   * open the operator and do initializations
   * */
  public final void open() throws DbException {
    // open the children first
    Operator[] children = getChildren();
    if (children != null) {
      for (Operator child : children) {
        if (child != null) {
          child.open();
        }
      }
    }
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
  protected final void setEOS() {
    eos = true;
  }

  /**
   * Do the initialization of this operator.
   * */
  protected abstract void init() throws DbException;

  /**
   * Do the clean up, release resources
   * */
  protected abstract void cleanup() throws DbException;

  /**
   * Generate next output TupleBatch if possible. Return null immediately if currently no output can be generated.
   * 
   * Do not block the execution thread in this method, including sleep, wait on locks, etc.
   * 
   * @throws DbException if any error occurs
   * */
  public abstract _TupleBatch fetchNextReady() throws DbException;

  /**
   * @return return the Schema of the output tuples of this operator
   * 
   * @throws DbException if any error occurs
   */
  public abstract Schema getSchema() throws DbException;

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
  protected abstract _TupleBatch fetchNext() throws DbException;

  /**
   * @return return the children Operators of this operator. If there is only one child, return an array of only one
   *         element. For join operators, the order of the children is not important. But they should be consistent
   *         among multiple calls.
   */
  public abstract Operator[] getChildren();

  /**
   * Set the children(child) of this operator. If the operator has only one child, children[0] should be used. If the
   * operator is a join, children[0] and children[1] should be used.
   * 
   * 
   * @param children the Operators which are to be set as the children(child) of this operator
   */
  public abstract void setChildren(Operator[] children);
}
