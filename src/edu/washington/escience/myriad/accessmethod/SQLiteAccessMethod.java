package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;

/**
 * Access method for a SQLite database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class SQLiteAccessMethod {

  /** Default busy timeout is one second. */
  private static final long DEFAULT_BUSY_TIMEOUT = 1000;

  /**
   * Wrap boolean values as int values since SQLite does not support boolean natively. This function converts true to 1
   * and false to 0.
   * 
   * @param b boolean to be converted to SQLite int
   * @return 1 if b is true; 0 if b is false
   */
  static int sqliteBooleanToInt(final boolean b) {
    if (b) {
      return 1;
    }
    return 0;
  }

  /**
   * Inserts a TupleBatch into the SQLite database.
   * 
   * @param pathToSQLiteDb filename of the SQLite database
   * @param insertString parameterized string used to insert tuples
   * @param tupleBatch TupleBatch that contains the data to be inserted
   */
  public static synchronized void tupleBatchInsert(final String pathToSQLiteDb, final String insertString,
      final TupleBatch tupleBatch) {
    SQLiteConnection sqliteConnection = null;
    SQLiteStatement statement = null;
    try {
      /* Connect to the database */
      sqliteConnection = new SQLiteConnection(new File(pathToSQLiteDb));
      sqliteConnection.open(false);
      sqliteConnection.setBusyTimeout(SQLiteAccessMethod.DEFAULT_BUSY_TIMEOUT);

      /* BEGIN TRANSACTION */
      sqliteConnection.exec("BEGIN TRANSACTION");

      /* Set up and execute the query */
      statement = sqliteConnection.prepare(insertString);

      tupleBatch.getIntoSQLite(statement);
      // for (int row = 0, totalTuples = tupleBatch.numTuples(); row < totalTuples; row++) {
      // for (int column = 0; column < tupleBatch.numColumns(); ++column) {
      // tupleBatch.getColumn(column).getIntoSQLite(row, statement, column + 1);
      // }
      // statement.step();
      // statement.reset();
      // }
      /* COMMIT TRANSACTION */
      sqliteConnection.exec("COMMIT TRANSACTION");

    } catch (final SQLiteException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    } finally {
      if (statement != null && !statement.isDisposed()) {
        statement.dispose();
      }
      if (sqliteConnection != null && !sqliteConnection.isDisposed()) {
        sqliteConnection.dispose();
      }
    }
  }

  /**
   * Create a SQLite Connection and then expose the results as an Iterator<TupleBatch>.
   * 
   * @param pathToSQLiteDb filename of the SQLite database
   * @param queryString string containing the SQLite query to be executed
   * @param schema the Schema describing the format of the TupleBatch containing these results.
   * @return an Iterator<TupleBatch> containing the results of the query
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String pathToSQLiteDb, final String queryString,
      final Schema schema) {
    try {
      /* Connect to the database */
      final SQLiteConnection sqliteConnection = new SQLiteConnection(new File(pathToSQLiteDb));
      sqliteConnection.open(false);

      /* Set up and execute the query */
      final SQLiteStatement statement = sqliteConnection.prepare(queryString);

      /* Step the statement once so we can figure out the Schema */
      statement.step();

      return new SQLiteTupleBatchIterator(statement, schema, sqliteConnection);
    } catch (final SQLiteException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /** Inaccessible. */
  private SQLiteAccessMethod() {
    throw new AssertionError();
  }

}

/**
 * Wraps a SQLiteStatement result set in a Iterator<TupleBatch>.
 * 
 * @author dhalperi
 * 
 */
class SQLiteTupleBatchIterator implements Iterator<TupleBatch> {
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
      // this.schema = Schema.fromSQLiteStatement(statement);
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
    final int numFields = schema.numFields();
    final List<Column<?>> columns = ColumnFactory.allocateColumns(schema);

    /**
     * Loop through resultSet, adding one row at a time. Stop when numTuples hits BATCH_SIZE or there are no more
     * results.
     */
    int numTuples;
    try {
      for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE && statement.hasRow(); ++numTuples) {
        for (int column = 0; column < numFields; ++column) {
          columns.get(column).putFromSQLite(statement, column);
        }
        statement.step();
      }
    } catch (final SQLiteException e) {
      System.err.println("Got SQLiteException:" + e + "in TupleBatchIterator.next()");
      throw new RuntimeException(e.getMessage());
    }

    return new TupleBatch(schema, columns, numTuples);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("SQLiteTupleBatchIterator.remove()");
  }
}