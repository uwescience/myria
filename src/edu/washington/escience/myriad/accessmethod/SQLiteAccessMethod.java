package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnBuilder;
import edu.washington.escience.myriad.column.ColumnFactory;

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
  protected SQLiteAccessMethod(final SQLiteInfo sqliteInfo, final Boolean readOnly) throws DbException {
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

    File dbFile = new File(sqliteInfo.getDatabase());
    if (!dbFile.exists()) {
      throw new DbException("Database file " + sqliteInfo.getDatabase() + " does not exist!");
    }

    sqliteConnection = null;
    try {
      sqliteConnection = new SQLiteConnection(new File(sqliteInfo.getDatabase()));
      if (readOnly) {
        sqliteConnection.openReadonly();
      } else {
        sqliteConnection.open(false);
      }
      sqliteConnection.setBusyTimeout(SQLiteAccessMethod.DEFAULT_BUSY_TIMEOUT);
    } catch (final SQLiteException e) {
      LOGGER.error(e.getErrorCode() + "-" + e.getMessage());
      throw new DbException(e.getErrorCode() + "-" + e.getMessage() + " filename: " + sqliteInfo.getDatabase());
    }
  }

  public void init() throws DbException {
    execute("PRAGMA journal_mode=WAL;");
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
    Objects.requireNonNull(sqliteConnection);

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
      throw new DbException(e.getMessage());
    } finally {
      if (statement != null && !statement.isDisposed()) {
        statement.dispose();
      }
    }
  }

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
        if (count >= 1000) {
          LOGGER.error(e.getMessage());
          throw new DbException(e);
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }

    try {
      /* Step the statement once so we can figure out the Schema */
      statement.step();
    } catch (final SQLiteException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e);
    }

    return new SQLiteTupleBatchIterator(statement, schema, sqliteConnection);
  }

  @Override
  public void execute(final String ddlCommand) throws DbException {
    Objects.requireNonNull(sqliteConnection);

    try {
      sqliteConnection.exec(ddlCommand);
    } catch (final SQLiteException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e.getMessage());
    }
  }

  @Override
  public void close() throws DbException {
    if (sqliteConnection != null) {
      sqliteConnection.dispose();
      sqliteConnection = null;
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
      throw new RuntimeException(e.getMessage());
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
      throw new RuntimeException(e.getMessage());
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