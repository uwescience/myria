package edu.washington.escience.myriad.table;

import java.io.Serializable;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;

public interface DbTable extends Serializable {

  /**
   * Closes the iterator. When the iterator is closed, calling next(), hasNext(), or rewind() should fail by throwing
   * IllegalStateException.
   */
  public void close() throws DbException;

  /**
   * Returns the Schema associated with this DbIterator.
   * 
   * @return the Schema associated with this DbIterator.
   */
  public Schema getSchema();

  /**
   * Opens the iterator. This must be called before any of the other methods.
   * 
   * @throws DbException when there are problems opening/accessing the database.
   */
  public void open() throws DbException;

}
