/**
 * 
 */
package edu.washington.escience.myria.accessmethod;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;

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
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
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
  public abstract void tupleBatchInsert(final String insertString, final TupleBatch tupleBatch) throws DbException;

  /**
   * Runs a query and expose the results as an Iterator<TupleBatch>.
   * 
   * @param queryString the query
   * @param schema the output schema (with SQLite we are not able to reconstruct the schema from the API)
   * @return an Iterator<TupleBatch> containing the results.
   * @throws DbException if there is an error getting tuples.
   */
  public abstract Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString, final Schema schema)
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
  public abstract void close() throws DbException;

  /**
   * Perform some initialization steps for the specific database system.
   * 
   * @throws DbException if there is an error in the database.
   */
  public void init() throws DbException {
  }

  /**
   * Generates the insert statement string for a relation in the database.
   * 
   * @param schema the relation schema
   * @param relationKey the relation name
   * @return the insert statement string
   */
  public abstract String insertStatementFromSchema(Schema schema, RelationKey relationKey);

  /**
   * Generates the create table statement string for a relation in the database.
   * 
   * @param schema the relation schema
   * @param relationKey the relation name
   * @return the create table statement string
   */
  public abstract String createIfNotExistsStatementFromSchema(Schema schema, RelationKey relationKey);

  /**
   * Creates a table in the database, if it does not already exist.
   * 
   * @param relationKey the relation name
   * @param schema the relation schema
   * @throws DbException if anything goes wrong
   */
  public abstract void createTableIfNotExists(RelationKey relationKey, Schema schema) throws DbException;

  /**
   * Overwrite <code>oldRelation</code> with <code>newRelation</code> by dropping <code>oldRelation</code> if it exists
   * and renaming <code>newRelation</code> to <code>oldRelation</code>.
   * 
   * @param oldRelation the table to be overwritten.
   * @param newRelation the table replacing <code>old</code>.
   * @throws DbException if there is an error during this operation.
   */
  public abstract void dropAndRenameTables(RelationKey oldRelation, RelationKey newRelation) throws DbException;

  /**
   * Drop the specified table, if it exists.
   * 
   * @param relationKey the table to be dropped.
   * @throws DbException if there is an error dropping the table.
   */
  public abstract void dropTableIfExists(RelationKey relationKey) throws DbException;

  /**
   * Holds a reference to a column and whether it is ascending or descending.
   */
  public final class IndexRef implements Serializable {

    /** Required for Java serialization. */
    private static final long serialVersionUID = 1L;
    /** Which column should be hashed. */
    @JsonProperty
    private final int column;
    /** True if the column should be hashed in ascending order. */
    @JsonProperty
    private final boolean ascending;

    /**
     * This is not really unused, it's used automagically by Jackson deserialization.
     */
    @SuppressWarnings("unused")
    private IndexRef() {
      column = -1;
      ascending = true;
    }

    /**
     * Constructs a new IndexRef.
     * 
     * @param column which column should be hashed.
     * @param ascending true if the column should be hashed in ascending order.
     */
    public IndexRef(final int column, final boolean ascending) {
      this.column = column;
      this.ascending = ascending;
    }

    /**
     * Constructs a new IndexRef.
     * 
     * @param column which column should be hashed.
     */
    public IndexRef(final int column) {
      this(column, true);
    }

    /**
     * @return which column should be hashed.
     */
    public int getColumn() {
      return column;
    }

    /**
     * @return true if the column should be hashed in ascending order.
     */
    public boolean isAscending() {
      return ascending;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder hb = new HashCodeBuilder();
      hb.append(ascending).append(column);
      return hb.toHashCode();
    }

    @Override
    public boolean equals(final Object o) {
      if (o == null) {
        return false;
      }
      if (!(o instanceof IndexRef)) {
        return false;
      }
      IndexRef other = (IndexRef) o;
      return ascending == other.ascending && column == other.column;
    }
  }
}