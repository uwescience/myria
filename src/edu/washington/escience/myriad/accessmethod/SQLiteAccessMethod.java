package edu.washington.escience.myriad.accessmethod;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Column;
import edu.washington.escience.myriad.DoubleColumn;
import edu.washington.escience.myriad.LongColumn;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.StringColumn;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

public class SQLiteAccessMethod {

  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(String pathToSQLiteDb,
      String queryString) {
    try {
      /* Connect to the database */
      SQLiteConnection SQLiteConnection = new SQLiteConnection(new File(pathToSQLiteDb));
      SQLiteConnection.open(false);

      /* Set up and execute the query */
      SQLiteStatement statement = SQLiteConnection.prepare(queryString);

      /* Step the statement once so we can figure out the Schema */
      statement.step();

      return new SQLiteTupleBatchIterator(statement, Schema.fromSQLiteStatement(statement));
    } catch (SQLiteException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  static int sqliteBooleanToInt(boolean b) {
    if (b)
      return 1;
    return 0;
  }

  public static void tupleBatchInsert(String pathToSQLiteDb, String insertString,
      TupleBatch tupleBatch) {
    try {
      /* Extract the Schema */
      Schema schema = tupleBatch.getSchema();

      /* Connect to the database */
      SQLiteConnection SQLiteConnection = new SQLiteConnection(new File(pathToSQLiteDb));
      SQLiteConnection.open(false);

      /* BEGIN TRANSACTION */
      SQLiteConnection.exec("BEGIN TRANSACTION");

      /* Set up and execute the query */
      SQLiteStatement statement = SQLiteConnection.prepare(insertString);

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
      SQLiteConnection.exec("COMMIT TRANSACTION");
      SQLiteConnection.dispose();
    } catch (SQLiteException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

}

/**
 * Wraps a SQLite ResultSet in a Iterator<TupleBatch>.
 * 
 * @author dhalperi
 * 
 */
class SQLiteTupleBatchIterator implements Iterator<TupleBatch> {
  private final SQLiteStatement statement;
  private final Schema schema;

  SQLiteTupleBatchIterator(SQLiteStatement statement) {
    this.statement = statement;
    try {
      if (!statement.hasStepped())
        statement.step();
      this.schema = Schema.fromSQLiteStatement(statement);
    } catch (SQLiteException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  SQLiteTupleBatchIterator(SQLiteStatement statement, Schema schema) {
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
    List<Column> columns = Column.allocateColumns(schema);

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
        if (!statement.step())
          break;
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