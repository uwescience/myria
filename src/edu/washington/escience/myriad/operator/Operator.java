package edu.washington.escience.myriad.operator;

import java.util.NoSuchElementException;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * Abstract class for implementing operators. It handles <code>close</code>, <code>next</code> and <code>hasNext</code>.
 * Subclasses only need to implement <code>open</code> and <code>readNext</code>.
 */
public abstract class Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  private _TupleBatch next = null;

  private boolean open = false;

  private int estimatedCardinality = 0;

  /**
   * Closes this iterator. If overridden by a subclass, they should call super.close() in order for Operator's internal
   * state to be consistent.
   */
  public void close() {
    // Ensures that a future call to next() will fail
    next = null;
    this.open = false;
  }

  /**
   * Returns the next Tuple in the iterator, or null if the iteration is finished. Operator uses this method to
   * implement both <code>next</code> and <code>hasNext</code>.
   * 
   * @return the next Tuple in the iterator, or null if the iteration is finished.
   */
  protected abstract _TupleBatch fetchNext() throws DbException;

  /**
   * @return return the children DbIterators of this operator. If there is only one child, return an array of only one
   *         element. For join operators, the order of the children is not important. But they should be consistent
   *         among multiple calls.
   */
  public abstract Operator[] getChildren();

  /**
   * @return The estimated cardinality of this operator. Will only be used in lab6
   */
  public int getEstimatedCardinality() {
    return this.estimatedCardinality;
  }

  /**
   * @return return the Schema of the output tuples of this operator
   */
  public abstract Schema getSchema();

  public boolean hasNext() throws DbException {
    if (!this.open) {
      throw new IllegalStateException("Operator not yet open");
    }

    if (next == null) {
      next = fetchNext();
    }
    return next != null;
  }

  public _TupleBatch next() throws DbException, NoSuchElementException {
    if (next == null) {
      next = fetchNext();
      if (next == null) {
        throw new NoSuchElementException();
      }
    }

    final _TupleBatch result = next;
    next = null;
    return result;
  }

  public void open() throws DbException {
    this.open = true;
  }

  /**
   * Set the children(child) of this operator. If the operator has only one child, children[0] should be used. If the
   * operator is a join, children[0] and children[1] should be used.
   * 
   * 
   * @param children the DbIterators which are to be set as the children(child) of this operator
   */
  public abstract void setChildren(Operator[] children);

  /**
   * @param card The estimated cardinality of this operator Will only be used in lab6
   */
  protected void setEstimatedCardinality(final int card) {
    this.estimatedCardinality = card;
  }

}
