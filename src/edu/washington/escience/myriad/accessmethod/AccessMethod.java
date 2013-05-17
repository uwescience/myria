/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatch;

/**
 * . Interface for Database Access Methods
 * 
 * @author valmeida
 * 
 */
public interface AccessMethod {

  /**
   * . Connects with the database
   * 
   * @param connectionInfo connection information
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  void connect(final ConnectionInfo connectionInfo, final Boolean readOnly) throws DbException;

  /**
   * Sets the connection to be read-only or writable.
   * 
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  void setReadOnly(final Boolean readOnly) throws DbException;

  /**
   * Insert the tuples in this TupleBatch into the database.
   * 
   * @param insertString the insert statement.
   * @param tupleBatch the tupleBatch to be inserted
   * @throws DbException if there is an error inserting the tuples.
   */
  void tupleBatchInsert(final String insertString, final TupleBatch tupleBatch) throws DbException;

  /**
   * Runs a query and expose the results as an Iterator<TupleBatch>.
   * 
   * @param queryString the query
   * @return an Iterator<TupleBatch> containing the results.
   * @throws DbException if there is an error getting tuples.
   */
  Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString) throws DbException;

  /**
   * Executes a DDL command.
   * 
   * @param ddlCommand the DDL command
   * @throws DbException if there is an error in the database.
   */
  void execute(final String ddlCommand) throws DbException;

  /**
   * Closes the database connection.
   * 
   * @throws DbException if there is an error in the database.
   */
  void close() throws DbException;

}
