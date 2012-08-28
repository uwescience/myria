package edu.washington.escience.myriad.accessmethod;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;

import edu.washington.escience.myriad.BooleanColumn;
import edu.washington.escience.myriad.Column;
import edu.washington.escience.myriad.DoubleColumn;
import edu.washington.escience.myriad.FloatColumn;
import edu.washington.escience.myriad.IntColumn;
import edu.washington.escience.myriad.LongColumn;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.StringColumn;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

/**
 * Access method for a JDBC database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcAccessMethod {

  /** Inaccessible. */
  private JdbcAccessMethod() {
    throw new AssertionError();
  }

  /**
   * Insert the Tuples in this TupleBatch into the database.
   * 
   * @param driverClassName the JDBC driver name
   * @param connectionString the string identifying the path to the database
   * @param insertString the insert statement. TODO No sanity checks at all right now.
   * @param tupleBatch the tupleBatch to be inserted
   */
  public static void tupleBatchInsert(final String driverClassName, final String connectionString,
      final String insertString, final TupleBatch tupleBatch) {
    try {
      /* Extract the Schema */
      Schema schema = tupleBatch.getSchema();

      /* Make sure JDBC driver is loaded */
      Class.forName(driverClassName);
      /* Connect to the database */
      Connection jdbcConnection = DriverManager.getConnection(connectionString);

      /* Set up and execute the query */
      PreparedStatement statement = jdbcConnection.prepareStatement(insertString);

      Type[] types = schema.getTypes();

      int curColumn;
      for (int row : tupleBatch.validTupleIndices()) {
        curColumn = 0;
        for (int column = 0; column < tupleBatch.numColumns(); ++column) {
          switch (types[column]) {
            case BOOLEAN_TYPE:
              statement.setBoolean(curColumn + 1, tupleBatch.getBoolean(column, row));
              break;
            case DOUBLE_TYPE:
              statement.setDouble(curColumn + 1, tupleBatch.getDouble(column, row));
              break;
            case FLOAT_TYPE:
              statement.setFloat(curColumn + 1, tupleBatch.getFloat(column, row));
              break;
            case INT_TYPE:
              statement.setInt(curColumn + 1, tupleBatch.getInt(column, row));
              break;
            case LONG_TYPE:
              statement.setLong(curColumn + 1, tupleBatch.getLong(column, row));
              break;
            case STRING_TYPE:
              statement.setString(curColumn + 1, tupleBatch.getString(column, row));
              break;
          }
          curColumn++;
        }
        statement.addBatch();
      }
      statement.executeBatch();
      statement.close();
      jdbcConnection.close();
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Create a JDBC Connection and then expose the results as an Iterator<TupleBatch>.
   * 
   * @param driverClassName the JDBC driver name
   * @param connectionString the string identifying the path to the database
   * @param queryString the query
   * @return an Iterator<TupleBatch> containing the results.
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String driverClassName,
      final String connectionString, final String queryString) {
    try {
      /* Make sure JDBC driver is loaded */
      Class.forName(driverClassName);
      /* Connect to the database */
      Connection jdbcConnection = DriverManager.getConnection(connectionString);
      /* Set read only on the connection */
      jdbcConnection.setReadOnly(true);

      /* Set up and execute the query */
      Statement statement = jdbcConnection.createStatement();
      ResultSet resultSet = statement.executeQuery(queryString);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      return new JdbcTupleBatchIterator(resultSet, Schema.fromResultSetMetaData(resultSetMetaData));
    } catch (ClassNotFoundException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }
}

/**
 * Wraps a JDBC ResultSet in a Iterator<TupleBatch>.
 * 
 * Implementation based on org.apache.commons.dbutils.ResultSetIterator. Requires ResultSet.isLast()
 * to be implemented.
 * 
 * @author dhalperi
 * 
 */
class JdbcTupleBatchIterator implements Iterator<TupleBatch> {
  /** The results from a JDBC query that will be returned in TupleBatches by this Iterator. */
  private final ResultSet resultSet;
  /** The Schema of the TupleBatches returned by this Iterator. */
  private final Schema schema;

  /**
   * Constructs a JdbcTupleBatchIterator from the given ResultSet and Schema objects.
   * 
   * @param resultSet the JDBC ResultSet containing the results.
   * @param schema the Schema of the generated TupleBatch objects.
   */
  JdbcTupleBatchIterator(final ResultSet resultSet, final Schema schema) {
    this.resultSet = resultSet;
    this.schema = schema;
  }

  @Override
  public boolean hasNext() {
    try {
      return !(resultSet.isClosed() || resultSet.isLast());
    } catch (SQLException e) {
      System.err.println("Dropping SQLException:" + e);
      return false;
    }
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
      for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE; ++numTuples) {
        if (!resultSet.next()) {
          Connection connection = resultSet.getStatement().getConnection();
          resultSet.getStatement().close();
          connection.close(); /* Also closes the resultSet */
          break;
        }
        for (int colIdx = 0; colIdx < numFields; ++colIdx) {
          int jdbcIdx = colIdx + 1;
          /* Warning: JDBC is 1-indexed */
          switch (fieldTypes[colIdx]) {
            case INT_TYPE:
              ((IntColumn) columns.get(colIdx)).putInt(resultSet.getInt(jdbcIdx));
              break;
            case STRING_TYPE:
              ((StringColumn) columns.get(colIdx)).putString(resultSet.getString(jdbcIdx));
              break;
            case BOOLEAN_TYPE:
              ((BooleanColumn) columns.get(colIdx)).putBoolean(resultSet.getBoolean(jdbcIdx));
              break;
            case DOUBLE_TYPE:
              ((DoubleColumn) columns.get(colIdx)).putDouble(resultSet.getDouble(jdbcIdx));
              break;
            case FLOAT_TYPE:
              ((FloatColumn) columns.get(colIdx)).putFloat(resultSet.getFloat(jdbcIdx));
              break;
            case LONG_TYPE:
              ((LongColumn) columns.get(colIdx)).putLong(resultSet.getLong(jdbcIdx));
              break;
          }
        }
      }
    } catch (SQLException e) {
      System.err.println("Got SQLException:" + e + "in JdbcTupleBatchIterator.next()");
      throw new RuntimeException(e.getMessage());
    }

    return new TupleBatch(schema, columns, numTuples);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("JdbcTupleBatchIterator.remove()");
  }
}
