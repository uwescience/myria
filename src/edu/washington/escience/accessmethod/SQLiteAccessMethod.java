package edu.washington.escience.accessmethod;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.Column;
import edu.washington.escience.IntColumn;
import edu.washington.escience.Schema;
import edu.washington.escience.StringColumn;
import edu.washington.escience.TupleBatch;
import edu.washington.escience.Type;

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
}

/**
 * Wraps a SQLite ResultSet in a Iterator<TupleBatch>.
 * 
 * @author dhalperi
 * 
 */
class SQLiteTupleBatchIterator implements Iterator<TupleBatch> {
  private SQLiteStatement statement;
  private Schema schema;

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
    Type[] fieldTypes = schema.getTypes();
    List<Column> columns = Column.allocateColumns(schema);

    /**
     * Loop through resultSet, adding one row at a time. Stop when numTuples hits BATCH_SIZE or
     * there are no more results.
     */
    int numTuples;
    try {
      for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE && statement.hasRow(); ++numTuples) {
        for (int fieldIndex = 0; fieldIndex < numFields; ++fieldIndex) {
          if (fieldTypes[fieldIndex] == Type.INT_TYPE) {
            ((IntColumn) columns.get(fieldIndex)).putInt(statement.columnInt(fieldIndex));
          } else if (fieldTypes[fieldIndex] == Type.STRING_TYPE) {
            ((StringColumn) columns.get(fieldIndex)).putString(statement.columnString(fieldIndex));
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