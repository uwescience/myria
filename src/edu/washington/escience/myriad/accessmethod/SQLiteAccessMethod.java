package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;

/**
 * Access method for a SQLite database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class SQLiteAccessMethod {

  /**
   * Wrap boolean values as int values since SQLite does not support boolean natively. This function
   * converts true to 1 and false to 0.
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
  public static void tupleBatchInsert(final String pathToSQLiteDb, final String insertString,
      final TupleBatch tupleBatch) {
    try {
      /* Extract the Schema */
      Schema schema = tupleBatch.getSchema();

      /* Connect to the database */
      SQLiteConnection sqliteConnection = new SQLiteConnection(new File(pathToSQLiteDb));
      sqliteConnection.open(false);

      /* BEGIN TRANSACTION */
      sqliteConnection.exec("BEGIN TRANSACTION");

      /* Set up and execute the query */
      SQLiteStatement statement = sqliteConnection.prepare(insertString);

      Type[] types = schema.getTypes();

      int curColumn;
      for (int row : tupleBatch.validTupleIndices()) {
        curColumn = 0;
        for (int column = 0; column < tupleBatch.numColumns(); ++column) {
          switch (types[column]) {
            case DOUBLE_TYPE:
              statement.bind(curColumn + 1, tupleBatch.getDouble(column, row));
              break;
            case FLOAT_TYPE:
              /* Will be stored as 8 bytes */
              statement.bind(curColumn + 1, tupleBatch.getFloat(column, row));
              break;
            case INT_TYPE:
              statement.bind(curColumn + 1, tupleBatch.getInt(column, row));
              break;
            case LONG_TYPE:
              statement.bind(curColumn + 1, tupleBatch.getLong(column, row));
              break;
            case STRING_TYPE:
              statement.bind(curColumn + 1, tupleBatch.getString(column, row));
              break;
            case BOOLEAN_TYPE:
              throw new RuntimeException("SQLite does not support Boolean columns");
              // statement.bind(curColumn + 1,
              // sqliteBooleanToInt(tupleBatch.getBoolean(column, row)));
          }
          curColumn++;
        }
        statement.step();
        statement.reset();
      }
      /* BEGIN TRANSACTION */
      sqliteConnection.exec("COMMIT TRANSACTION");
      sqliteConnection.dispose();
    } catch (SQLiteException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Create a SQLite Connection and then expose the results as an Iterator<TupleBatch>.
   * 
   * @param pathToSQLiteDb filename of the SQLite database
   * @param queryString string containing the SQLite query to be executed
   * @return an Iterator<TupleBatch> containing the results of the query
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String pathToSQLiteDb,
      final String queryString) {
    try {
      /* Connect to the database */
      SQLiteConnection sqliteConnection = new SQLiteConnection(new File(pathToSQLiteDb));
      sqliteConnection.open(false);

      /* Set up and execute the query */
      SQLiteStatement statement = sqliteConnection.prepare(queryString);

      /* Step the statement once so we can figure out the Schema */
      statement.step();

      return new SQLiteTupleBatchIterator(statement, Schema.fromSQLiteStatement(statement));
    } catch (SQLiteException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
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
  /** The Schema of the TupleBatches returned by this Iterator. */
  private final Schema schema;

  /**
   * Wraps a SQLiteStatement result set in an Iterator<TupleBatch>.
   * 
   * @param statement the SQLiteStatement containing the results. If it has not yet stepped, this
   *          constructor will step it. Then the Schema of the generated TupleBatchs will be
   *          extracted from the statement.
   */
  SQLiteTupleBatchIterator(final SQLiteStatement statement) {
    this.statement = statement;
    try {
      if (!statement.hasStepped()) {
        statement.step();
      }
      this.schema = Schema.fromSQLiteStatement(statement);
    } catch (SQLiteException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Wraps a SQLiteStatement result set in an Iterator<TupleBatch>.
   * 
   * @param statement the SQLiteStatement containing the results.
   * @param schema the Schema describing the format of the TupleBatch containing these results.
   */
  SQLiteTupleBatchIterator(final SQLiteStatement statement, final Schema schema) {
    this.statement = statement;
    this.schema = schema;
  }

  @Override
  public boolean hasNext() {
    return statement.hasRow();
  }

  @Override
  public TupleBatch next() {
    /* Allocate TupleBatch parameters */
    int numFields = schema.numFields();
    Type[] types = schema.getTypes();
    List<Column> columns = ColumnFactory.allocateColumns(schema);

    /**
     * Loop through resultSet, adding one row at a time. Stop when numTuples hits BATCH_SIZE or
     * there are no more results.
     */
    int numTuples;
    try {
      for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE && statement.hasRow(); ++numTuples) {
        for (int column = 0; column < numFields; ++column) {
          switch (types[column]) {
            case BOOLEAN_TYPE:
              throw new RuntimeException("SQLite does not support Boolean columns");
            case DOUBLE_TYPE:
              ((DoubleColumn) columns.get(column)).putDouble(statement.columnDouble(column));
              break;
            case FLOAT_TYPE:
              throw new RuntimeException("SQLite does not support Float columns");
            case INT_TYPE:
              throw new RuntimeException("SQLite does not support Integer columns");
              // ((IntColumn) columns.get(column)).putInt(statement.columnInt(column));
              // break;
            case LONG_TYPE:
              ((LongColumn) columns.get(column)).putLong(statement.columnLong(column));
              break;
            case STRING_TYPE:
              ((StringColumn) columns.get(column)).putString(statement.columnString(column));
              break;
          }
        }
        if (!statement.step()) {
          break;
        }
      }
    } catch (SQLiteException e) {
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