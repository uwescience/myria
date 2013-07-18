/**
 * 
 */
package edu.washington.escience.myriad.accessmethod;

import java.util.Iterator;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;

/**
 * Interface for Database Access Methods.
 * 
 * @author valmeida
 * 
 */
public abstract class AccessMethod {

  /**
   * Factory.
   * 
   * @param dbms the DBMS
   * @param connectionInfo the connection info
   * @param readOnly the flag for read-only
   * @return the object of type AccessMethod
   * @throws DbException if anything goes wrong on connecting
   */
  public static AccessMethod of(final String dbms, final ConnectionInfo connectionInfo, final Boolean readOnly)
      throws DbException {
    switch (dbms) {
      case MyriaConstants.STORAGE_SYSTEM_SQLITE:
        return new SQLiteAccessMethod((SQLiteInfo) connectionInfo, readOnly);
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
      case MyriaConstants.STORAGE_SYSTEM_VERTICA:
        return new JdbcAccessMethod((JdbcInfo) connectionInfo, readOnly);
    }
    return null;
  }

  /**
   * Connects with the database.
   * 
   * @param connectionInfo connection information
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  abstract void connect(final ConnectionInfo connectionInfo, final Boolean readOnly) throws DbException;

  /**
   * Sets the connection to be read-only or writable.
   * 
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  abstract void setReadOnly(final Boolean readOnly) throws DbException;

  /**
   * Insert the tuples in this TupleBatch into the database.
   * 
   * @param insertString the insert statement.
   * @param tupleBatch the tupleBatch to be inserted
   * @throws DbException if there is an error inserting the tuples.
   */
  abstract public void tupleBatchInsert(final String insertString, final TupleBatch tupleBatch) throws DbException;

  /**
   * Runs a query and expose the results as an Iterator<TupleBatch>.
   * 
   * @param queryString the query
   * @param schema the output schema (with SQLite we are not able to reconstruct the schema from the API)
   * @return an Iterator<TupleBatch> containing the results.
   * @throws DbException if there is an error getting tuples.
   */
  abstract public Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString, final Schema schema)
      throws DbException;

  /**
   * Executes a DDL command.
   * 
   * @param ddlCommand the DDL command
   * @throws DbException if there is an error in the database.
   */
  abstract void execute(final String ddlCommand) throws DbException;

  /**
   * Closes the database connection.
   * 
   * @throws DbException if there is an error in the database.
   */
  abstract void close() throws DbException;

}
