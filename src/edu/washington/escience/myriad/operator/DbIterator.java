package edu.washington.escience.myriad.operator;

import java.io.Serializable;
import java.util.NoSuchElementException;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * DbIterator is the iterator interface that all SimpleDB operators should implement. If the iterator is not open, none
 * of the methods should work, and should throw an IllegalStateException. In addition to any resource
 * allocation/deallocation, an open method should call any child iterator open methods, and in a close method, an
 * iterator should call its children's close methods.
 */
public interface DbIterator extends Serializable {
  /**
   * Closes the iterator. When the iterator is closed, calling next(), hasNext(), or rewind() should fail by throwing
   * IllegalStateException.
   */
  void close();

  /**
   * Returns the Schema associated with this DbIterator.
   * 
   * @return the Schema associated with this DbIterator.
   */
  Schema getSchema();

  /**
   * Returns true if the iterator has more tuples.
   * 
   * @return true f the iterator has more tuples.
   * @throws IllegalStateException If the iterator has not been opened
   */
  boolean hasNext() throws DbException;

  /**
   * Returns the next tuple from the operator (typically implementing by reading from a child operator or an access
   * method).
   * 
   * @return the next tuple in the iteration.
   * @throws NoSuchElementException if there are no more tuples.
   * @throws IllegalStateException If the iterator has not been opened
   */
  _TupleBatch next() throws DbException, NoSuchElementException;

  /**
   * Opens the iterator. This must be called before any of the other methods.
   * 
   * @throws DbException when there are problems opening/accessing the database.
   */
  void open() throws DbException;

  /**
   * Resets the iterator to the start.
   * 
   * @throws DbException when rewind is unsupported.
   * @throws IllegalStateException if the iterator has not been opened
   */
  // public void rewind() throws DbException;

}
