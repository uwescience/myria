/**
 *
 */
package edu.washington.escience.myria.accessmethod;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Wraps a SQLiteStatement result set in a Iterator<TupleBatch>.
 *
 */
public class SQLiteTupleBatchIterator implements Iterator<TupleBatch> {
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
   * @param statement the SQLiteStatement containing the results. If it has not yet stepped, this constructor will step
   *          it. Then the Schema of the generated TupleBatchs will be extracted from the statement.
   * @param connection the connection to the SQLite database.
   * @param schema the Schema describing the format of the TupleBatch containing these results.
   */
  public SQLiteTupleBatchIterator(
      final SQLiteStatement statement, final SQLiteConnection connection, final Schema schema) {
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
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

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
