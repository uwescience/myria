/**
 * 
 */
package edu.washington.escience.myriad.operator;

import java.util.Objects;

import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.AccessMethod;
import edu.washington.escience.myriad.accessmethod.ConnectionInfo;

/**
 * @author valmeida
 * 
 */
public class DbInsert extends RootOperator {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DbInsert.class);
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The connection to the database database. */
  private AccessMethod accessMethod;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;
  /** The name of the table the tuples should be inserted into. */
  private final RelationKey relationKey;
  /** Whether to overwrite an existing table or not. */
  private final boolean overwriteTable;
  /** The statement used to insert tuples into the database. */
  private String insertString;

  /**
   * Constructs an insertion operator to store the tuples from the specified child in a SQLite database in the specified
   * file. If the table does not exist, it will be created; if it does exist then old data will persist and new data
   * will be inserted.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   */
  public DbInsert(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo) {
    this(child, relationKey, connectionInfo, false);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child in a SQLite database in the specified
   * file. If the table does not exist, it will be created; if it does exist then old data will persist and new data
   * will be inserted.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param overwriteTable whether to overwrite a table that already exists.
   */
  public DbInsert(final Operator child, final RelationKey relationKey, final boolean overwriteTable) {
    this(child, relationKey, null, overwriteTable);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child in a SQLite database in the specified
   * file. If the table does not exist, it will be created. If the table exists and overwriteTable is true, the existing
   * data will be dropped from the database before the new data is inserted. If overwriteTable is false, any existing
   * data will remain and new data will be appended.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param connectionInfo the parameters of the database connection.
   * @param overwriteTable whether to overwrite a table that already exists.
   */
  public DbInsert(final Operator child, final RelationKey relationKey, final ConnectionInfo connectionInfo,
      final boolean overwriteTable) {
    super(child);
    Objects.requireNonNull(child);
    Objects.requireNonNull(relationKey);
    this.connectionInfo = connectionInfo;
    this.relationKey = relationKey;
    this.overwriteTable = overwriteTable;
  }

  @Override
  public void cleanup() {
    try {
      if (accessMethod != null) {
        accessMethod.close();
      }
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tupleBatch) throws DbException {
    Objects.requireNonNull(accessMethod);
    Objects.requireNonNull(insertString);
    accessMethod.tupleBatchInsert(insertString, tupleBatch);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    /* retrieve connection information from the environment variables, if not already set */
    if (connectionInfo == null && execEnvVars != null) {
      LOGGER.info("DbInsert: Retrieving connection info from environment variables");
      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    }

    if (connectionInfo == null) {
      throw new DbException("Unable to instantiate DbInsert: connection information unknown");
    }
    LOGGER.info("DbInsert: connection info " + connectionInfo.toJson());

    /* open the JDBC Connection */
    accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
    /* Set up the insert statement. */
    insertString = accessMethod.insertStatementFromSchema(getSchema(), relationKey);
    /* create the table */
    accessMethod.createTable(relationKey, getSchema(), overwriteTable);
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }

}
