package edu.washington.escience.myria.accessmethod;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteQueue;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Access method for a SQLite database. Exposes data as TupleBatches.
 *
 *
 */
public final class SQLiteAccessMethod extends AccessMethod {

  /** Default busy timeout is one second. */
  private static final long DEFAULT_BUSY_TIMEOUT = 1000;
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteAccessMethod.class);
  /** The database connection. **/
  private SQLiteConnection sqliteConnection;
  /** The database queue, for database updates. */
  private SQLiteQueue sqliteQueue;
  /** The connection information. **/
  private SQLiteInfo sqliteInfo;
  /** Flag that identifies the connection type (read-only or not). **/
  private Boolean readOnly;

  /**
   * The constructor. Creates an object and connects with the database
   *
   * @param sqliteInfo connection information
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  public SQLiteAccessMethod(final SQLiteInfo sqliteInfo, final Boolean readOnly)
      throws DbException {
    Objects.requireNonNull(sqliteInfo);

    this.sqliteInfo = sqliteInfo;
    this.readOnly = readOnly;
    connect(sqliteInfo, readOnly);
  }

  @Override
  public void connect(final ConnectionInfo connectionInfo, final Boolean readOnly)
      throws DbException {
    Objects.requireNonNull(connectionInfo);

    this.readOnly = readOnly;
    sqliteInfo = (SQLiteInfo) connectionInfo;

    File dbFile = new File(sqliteInfo.getDatabaseFilename());
    if (!dbFile.exists()) {
      if (!readOnly) {
        try {
          File parent = dbFile.getParentFile();
          if (parent != null && !parent.exists()) {
            dbFile.getParentFile().mkdirs();
          }
          dbFile.createNewFile();
        } catch (IOException e) {
          throw new DbException("Could not create database file" + dbFile.getAbsolutePath(), e);
        }
      } else {
        throw new DbException(
            "Database file " + sqliteInfo.getDatabaseFilename() + " does not exist!");
      }
    }

    sqliteConnection = null;
    sqliteQueue = null;

    try {
      if (readOnly) {
        sqliteConnection = new SQLiteConnection(new File(sqliteInfo.getDatabaseFilename()));
        sqliteConnection.openReadonly();
        sqliteConnection.setBusyTimeout(SQLiteAccessMethod.DEFAULT_BUSY_TIMEOUT);
      } else {
        sqliteQueue = new SQLiteQueue(new File(sqliteInfo.getDatabaseFilename())).start();
      }
    } catch (final SQLiteException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void setReadOnly(final Boolean readOnly) throws DbException {
    Objects.requireNonNull(sqliteConnection);
    Objects.requireNonNull(sqliteInfo);

    if (this.readOnly != readOnly) {
      close();
      connect(sqliteInfo, readOnly);
    }
  }

  @Override
  public void tupleBatchInsert(final RelationKey relationKey, final TupleBatch tupleBatch)
      throws DbException {
    Objects.requireNonNull(sqliteQueue);

    try {
      sqliteQueue
          .execute(
              new SQLiteJob<Object>() {
                @Override
                protected Object job(final SQLiteConnection sqliteConnection) throws DbException {
                  SQLiteStatement statement = null;
                  Schema schema = tupleBatch.getSchema();
                  try {
                    /* BEGIN TRANSACTION */
                    sqliteConnection.exec("BEGIN TRANSACTION");
                    /* Set up and execute the query */
                    statement =
                        sqliteConnection.prepare(insertStatementFromSchema(schema, relationKey));
                    for (int row = 0; row < tupleBatch.numTuples(); ++row) {
                      for (int col = 0; col < tupleBatch.numColumns(); ++col) {
                        switch (schema.getColumnType(col)) {
                          case BOOLEAN_TYPE:
                            /* In SQLite, booleans are integers represented as 0 (false) or 1 (true). */
                            int colVal = 0;
                            if (tupleBatch.getBoolean(col, row)) {
                              colVal = 1;
                            }
                            statement.bind(col + 1, colVal);
                            break;
                          case DATETIME_TYPE:
                            statement.bind(
                                col + 1,
                                tupleBatch.getDateTime(col, row).getMillis()); // SQLite long
                            break;
                          case DOUBLE_TYPE:
                            statement.bind(col + 1, tupleBatch.getDouble(col, row));
                            break;
                          case FLOAT_TYPE:
                            statement.bind(col + 1, tupleBatch.getFloat(col, row));
                            break;
                          case INT_TYPE:
                            statement.bind(col + 1, tupleBatch.getInt(col, row));
                            break;
                          case LONG_TYPE:
                            statement.bind(col + 1, tupleBatch.getLong(col, row));
                            break;
                          case STRING_TYPE:
                            statement.bind(col + 1, tupleBatch.getString(col, row));
                            break;
                        }
                      }
                      statement.step();
                      statement.reset();
                    }
                    /* COMMIT TRANSACTION */
                    sqliteConnection.exec("COMMIT TRANSACTION");
                  } catch (final SQLiteException e) {
                    LOGGER.error(e.getMessage());
                    throw new DbException(e);
                  } finally {
                    if (statement != null && !statement.isDisposed()) {
                      statement.dispose();
                    }
                  }
                  return null;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new DbException(e);
    }
  }

  /** How many times to try to open a database before we give up. Normal is 2-3, outside is 10 to 20. */
  private static final int MAX_RETRY_ATTEMPTS = 1000;

  @Override
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(
      final String queryString, final Schema schema) throws DbException {
    Objects.requireNonNull(sqliteConnection);
    Objects.requireNonNull(schema);

    /* Set up and execute the query */
    SQLiteStatement statement = null;
    /*
     * prepare() might throw an exception. My understanding is, when a connection starts in WAL mode, it will first
     * acquire an exclusive lock to check if there is -wal file to recover from. Usually the file is empty so the lock
     * is released pretty fast. However if another connection comes during the exclusive lock period, a
     * "database is locked" exception will still be thrown. The following code simply tries to call prepare again.
     */
    int count = 0;
    Throwable cause = null;
    while (statement == null && count < MAX_RETRY_ATTEMPTS) {
      try {
        statement = sqliteConnection.prepare(queryString);
      } catch (final SQLiteException e) {
        cause = e;
        count++;
        try {
          Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_10_MS);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          return Collections.<TupleBatch>emptyList().iterator();
        }
      }
    }

    if (statement == null) {
      LOGGER.error("SQLite database maximum retry attempts exceeded", cause);
      throw new DbException(cause);
    }

    try {
      /* Step the statement once so we can figure out the Schema */
      statement.step();
    } catch (final SQLiteException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }

    return new SQLiteTupleBatchIterator(statement, sqliteConnection, schema);
  }

  @Override
  public void execute(final String ddlCommand) throws DbException {
    Objects.requireNonNull(sqliteQueue);

    try {
      sqliteQueue
          .execute(
              new SQLiteJob<Object>() {
                @Override
                protected Object job(final SQLiteConnection sqliteConnection) throws DbException {
                  try {
                    sqliteConnection.exec(ddlCommand);
                  } catch (final SQLiteException e) {
                    LOGGER.error(e.getMessage(), e);
                    throw new DbException(e);
                  }
                  return null;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new DbException("Error executing DDL command: " + ddlCommand, e);
    }
  }

  @Override
  public void close() throws DbException {
    if (sqliteConnection != null) {
      sqliteConnection.dispose();
      sqliteConnection = null;
    }
    if (sqliteQueue != null) {
      try {
        sqliteQueue.stop(true).join();
        sqliteQueue = null;
      } catch (InterruptedException e) {
        throw new DbException(e);
      }
    }
  }

  /**
   * Inserts a TupleBatch into the SQLite database.
   *
   * @param sqliteInfo SQLite connection information
   * @param relationKey the table to insert into.
   * @param tupleBatch TupleBatch that contains the data to be inserted.
   * @throws DbException if there is an error in the database.
   */
  public static synchronized void tupleBatchInsert(
      final SQLiteInfo sqliteInfo, final RelationKey relationKey, final TupleBatch tupleBatch)
      throws DbException {

    SQLiteAccessMethod sqliteAccessMethod = null;
    try {
      sqliteAccessMethod = new SQLiteAccessMethod(sqliteInfo, false);
      sqliteAccessMethod.tupleBatchInsert(relationKey, tupleBatch);
    } catch (DbException e) {
      throw e;
    } finally {
      if (sqliteAccessMethod != null) {
        sqliteAccessMethod.close();
      }
    }
  }

  /**
   * Create a SQLite Connection and then expose the results as an Iterator<TupleBatch>.
   *
   * @param sqliteInfo the SQLite database connection information
   * @param queryString string containing the SQLite query to be executed
   * @param schema the Schema describing the format of the TupleBatch containing these results.
   * @return an Iterator<TupleBatch> containing the results of the query
   * @throws DbException if there is an error in the database.
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(
      final SQLiteInfo sqliteInfo, final String queryString, final Schema schema)
      throws DbException {

    SQLiteAccessMethod sqliteAccessMethod = null;
    try {
      sqliteAccessMethod = new SQLiteAccessMethod(sqliteInfo, true);
      return sqliteAccessMethod.tupleBatchIteratorFromQuery(queryString, schema);
    } catch (DbException e) {
      if (sqliteAccessMethod != null) {
        sqliteAccessMethod.close();
      }
      throw e;
    }
  }

  @Override
  public String insertStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    final StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ")
        .append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE))
        .append(" ([");
    sb.append(StringUtils.join(schema.getColumnNames(), "],["));
    sb.append("]) VALUES (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('?');
    }
    sb.append(");");
    return sb.toString();
  }

  @Override
  public String createIfNotExistsStatementFromSchema(
      final Schema schema, final RelationKey relationKey) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ")
        .append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE))
        .append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('[')
          .append(schema.getColumnName(i))
          .append("] ")
          .append(typeToSQLiteType(schema.getColumnType(i)));
    }
    sb.append(");");
    return sb.toString();
  }

  /**
   * Helper utility for creating SQLite CREATE TABLE statements.
   *
   * @param type a Myriad column type.
   * @return the name of the SQLite type that matches the given Myriad type.
   */
  public static String typeToSQLiteType(final Type type) {
    switch (type) {
      case BOOLEAN_TYPE:
        return "BOOLEAN";
      case DOUBLE_TYPE:
        return "DOUBLE";
      case FLOAT_TYPE:
        return "DOUBLE";
      case INT_TYPE:
        return "INTEGER";
      case LONG_TYPE:
        return "INTEGER";
      case STRING_TYPE:
        return "TEXT";
      default:
        throw new UnsupportedOperationException("Type " + type + " is not supported");
    }
  }

  @Override
  public void createTableIfNotExists(final RelationKey relationKey, final Schema schema)
      throws DbException {
    Objects.requireNonNull(sqliteQueue);
    Objects.requireNonNull(sqliteInfo);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);

    execute(createIfNotExistsStatementFromSchema(schema, relationKey));
  }

  @Override
  public void dropAndRenameTables(final RelationKey oldRelation, final RelationKey newRelation)
      throws DbException {
    dropTableIfExists(oldRelation);
    final String oldName = oldRelation.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE);
    final String newName = newRelation.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE);
    execute("ALTER TABLE " + newName + " RENAME TO " + oldName);
  }

  @Override
  public void dropTableIfExists(final RelationKey relationKey) throws DbException {
    execute("DROP TABLE IF EXISTS " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
  }

  @Override
  public void dropTableIfExistsCascade(final RelationKey relationKey) throws DbException {
    LOGGER.warn("SQLite does not implement DROP TABLE...CASCADE, attempting DROP TABLE instead");
    execute("DROP TABLE IF EXISTS " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
  }

  @Override
  public void createIndexes(
      final RelationKey relationKey, final Schema schema, final List<List<IndexRef>> indexes)
      throws DbException {

    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);
    Objects.requireNonNull(indexes);

    /*
     * All indexes go in a separate "program" that has the name "__myria_indexes_TIMESTAMP" appended to it. We need the
     * timestamp because SQLite doesn't have a rename indexes command.
     */
    final String indexProgramName =
        new StringBuilder(relationKey.getProgramName())
            .append("__myria_indexes_")
            .append(System.nanoTime())
            .toString();

    final String tempTableName = relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE);
    for (List<IndexRef> index : indexes) {
      Objects.requireNonNull(index);
      /* The inner loop builds two things: the relation name for the index, and the list of columns to be indexed. */
      StringBuilder name = new StringBuilder(relationKey.getRelationName());
      StringBuilder columns = new StringBuilder("(");
      boolean first = true;
      for (IndexRef i : index) {
        Objects.requireNonNull(i);
        Preconditions.checkElementIndex(i.getColumn(), schema.numColumns());
        name.append('_').append(i.getColumn());
        if (!first) {
          columns.append(',');
        }
        first = false;
        columns.append(schema.getColumnName(i.getColumn()));
        if (i.isAscending()) {
          columns.append(" ASC");
        } else {
          columns.append(" DESC");
        }
      }
      columns.append(')');

      RelationKey indexRelationKey =
          RelationKey.of(relationKey.getUserName(), indexProgramName, name.toString());
      final String indexName = indexRelationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE);

      StringBuilder statement = new StringBuilder();
      statement
          .append("CREATE INDEX ")
          .append(indexName)
          .append(" ON ")
          .append(tempTableName)
          .append(columns.toString());

      execute(statement.toString());
    }
  }

  @Override
  public void createIndexIfNotExists(
      final RelationKey relationKey, final Schema schema, final List<IndexRef> index)
      throws DbException {
    throw new UnsupportedOperationException(
        "create index if not exists is not supported in sqlite yet, implement me");
  }
}
