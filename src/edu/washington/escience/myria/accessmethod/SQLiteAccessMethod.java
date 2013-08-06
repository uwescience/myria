package edu.washington.escience.myria.accessmethod;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ColumnBuilder;
import edu.washington.escience.myria.column.ColumnFactory;

/**
 * Access method for a SQLite database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
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
  public SQLiteAccessMethod(final SQLiteInfo sqliteInfo, final Boolean readOnly) throws DbException {
    Objects.requireNonNull(sqliteInfo);

    this.sqliteInfo = sqliteInfo;
    this.readOnly = readOnly;
    connect(sqliteInfo, readOnly);
  }

  @Override
  public void connect(final ConnectionInfo connectionInfo, final Boolean readOnly) throws DbException {
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
        throw new DbException("Database file " + sqliteInfo.getDatabaseFilename() + " does not exist!");
      }
    }

    sqliteConnection = null;
    sqliteQueue = null;

    try {
      if (readOnly) {
        sqliteConnection = new SQLiteConnection(new File(sqliteInfo.getDatabaseFilename()));
        sqliteConnection.open(false);
        sqliteConnection.setBusyTimeout(SQLiteAccessMethod.DEFAULT_BUSY_TIMEOUT);
        sqliteConnection.exec("PRAGMA journal_mode=WAL;");
        sqliteConnection.dispose();
        sqliteConnection = new SQLiteConnection(new File(sqliteInfo.getDatabaseFilename()));
        sqliteConnection.openReadonly();
        sqliteConnection.setBusyTimeout(SQLiteAccessMethod.DEFAULT_BUSY_TIMEOUT);
      } else {
        sqliteQueue = new SQLiteQueue(new File(sqliteInfo.getDatabaseFilename())).start();
        execute("PRAGMA journal_mode=WAL;");
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
  public void tupleBatchInsert(final String insertString, final TupleBatch tupleBatch) throws DbException {
    Objects.requireNonNull(sqliteQueue);

    try {
      sqliteQueue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws DbException {
          SQLiteStatement statement = null;
          try {
            /* BEGIN TRANSACTION */
            sqliteConnection.exec("BEGIN TRANSACTION");
            /* Set up and execute the query */
            statement = sqliteConnection.prepare(insertString);
            tupleBatch.getIntoSQLite(statement);
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
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new DbException(e);
    }

  }

  /** How many times to try to open a database before we give up. Normal is 2-3, outside is 10 to 20. */
  private static final int MAX_RETRY_ATTEMPTS = 1000;

  @Override
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString, final Schema schema)
      throws DbException {
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
    boolean conflict = true;
    int count = 0;
    while (conflict) {
      conflict = false;
      try {
        statement = sqliteConnection.prepare(queryString);
      } catch (final SQLiteException e) {
        conflict = true;
        count++;
        if (count >= MAX_RETRY_ATTEMPTS) {
          LOGGER.error(e.getMessage(), e);
          throw new DbException(e);
        }
        try {
          Thread.sleep(MyriaConstants.SHORT_WAITING_INTERVAL_10_MS);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          return Collections.<TupleBatch> emptyList().iterator();
        }
      }
    }

    try {
      /* Step the statement once so we can figure out the Schema */
      statement.step();
    } catch (final SQLiteException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }

    return new SQLiteTupleBatchIterator(statement, schema, sqliteConnection);
  }

  @Override
  public void execute(final String ddlCommand) throws DbException {
    Objects.requireNonNull(sqliteQueue);

    try {
      sqliteQueue.execute(new SQLiteJob<Object>() {
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
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new DbException(e);
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
   * @param insertString parameterized string used to insert tuples
   * @param tupleBatch TupleBatch that contains the data to be inserted
   * @throws DbException if there is an error in the database.
   */
  public static synchronized void tupleBatchInsert(final SQLiteInfo sqliteInfo, final String insertString,
      final TupleBatch tupleBatch) throws DbException {

    SQLiteAccessMethod sqliteAccessMethod = null;
    try {
      sqliteAccessMethod = new SQLiteAccessMethod(sqliteInfo, false);
      sqliteAccessMethod.tupleBatchInsert(insertString, tupleBatch);
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
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final SQLiteInfo sqliteInfo, final String queryString,
      final Schema schema) throws DbException {

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
    sb.append("INSERT INTO ").append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)).append(" ([");
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
  public String createStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE)).append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append('[').append(schema.getColumnName(i)).append("] ").append(typeToSQLiteType(schema.getColumnType(i)));
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
  public void createTable(final RelationKey relationKey, final Schema schema, final boolean overwriteTable)
      throws DbException {
    Objects.requireNonNull(sqliteQueue);
    Objects.requireNonNull(sqliteInfo);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);

    try {
      execute("DROP TABLE IF EXISTS " + relationKey.toString(sqliteInfo.getDbms()) + ";");
    } catch (DbException e) {
      ; /* Skip. this is okay. */
    }
    execute(createStatementFromSchema(schema, relationKey));
  }
}

/**
 * Wraps a SQLiteStatement result set in a Iterator<TupleBatch>.
 * 
 * @author dhalperi
 * 
 */
class SQLiteTupleBatchIterator implements Iterator<TupleBatch> {
  /** The logger for this class. Uses SQLiteAccessMethod settings. */
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLiteAccessMethod.class);
  /** The results from a SQLite query that will be returned in TupleBatches by this Iterator. */
  private final SQLiteStatement statement;
  /** The connection to the SQLite database. */
  private final SQLiteConnection connection;
  /** The Schema of the TupleBatches returned by this Iterator. */
  private final Schema schema;

  /**
   * Wraps a SQLiteStatement result set in an Iterator<TupleBatch>.
   * 
   * @param statement the SQLiteStatement containing the results.
   * @param schema the Schema describing the format of the TupleBatch containing these results.
   * @param connection the connection to the SQLite database.
   */
  SQLiteTupleBatchIterator(final SQLiteStatement statement, final Schema schema, final SQLiteConnection connection) {
    this.statement = statement;
    this.connection = connection;
    this.schema = schema;
  }

  /**
   * Wraps a SQLiteStatement result set in an Iterator<TupleBatch>.
   * 
   * @param statement the SQLiteStatement containing the results. If it has not yet stepped, this constructor will step
   *          it. Then the Schema of the generated TupleBatchs will be extracted from the statement.
   * @param connection the connection to the SQLite database.
   * @param schema the Schema describing the format of the TupleBatch containing these results.
   */
  SQLiteTupleBatchIterator(final SQLiteStatement statement, final SQLiteConnection connection, final Schema schema) {
    this.connection = connection;
    this.statement = statement;
    try {
      if (!statement.hasStepped()) {
        statement.step();
      }
      this.schema = schema;
    } catch (final SQLiteException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean hasNext() {
    final boolean hasRow = statement.hasRow();
    if (!hasRow) {
      statement.dispose();
      connection.dispose();
    }
    return hasRow;
  }

  @Override
  public TupleBatch next() {
    /* Allocate TupleBatch parameters */
    final int numFields = schema.numColumns();
    final List<ColumnBuilder<?>> columnBuilders = ColumnFactory.allocateColumns(schema);

    /**
     * Loop through resultSet, adding one row at a time. Stop when numTuples hits BATCH_SIZE or there are no more
     * results.
     */
    int numTuples;
    try {
      for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE && statement.hasRow(); ++numTuples) {
        for (int column = 0; column < numFields; ++column) {
          columnBuilders.get(column).appendFromSQLite(statement, column);
        }
        statement.step();
      }
    } catch (final SQLiteException e) {
      LOGGER.error("Got SQLiteException:" + e + "in TupleBatchIterator.next()");
      throw new RuntimeException(e);
    }

    List<Column<?>> columns = new ArrayList<Column<?>>(columnBuilders.size());
    for (ColumnBuilder<?> cb : columnBuilders) {
      columns.add(cb.build());
    }

    return new TupleBatch(schema, columns, numTuples);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("SQLiteTupleBatchIterator.remove()");
  }
}
