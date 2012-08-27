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

import edu.washington.escience.myriad.Column;
import edu.washington.escience.myriad.IntColumn;
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
public class JdbcAccessMethod {

  /**
   * Insert the Tuples in this TupleBatch into the database.
   * 
   * @param driverClassName the JDBC driver name
   * @param connectionString the string identifying the path to the database
   * @param insertString the insert statement. TODO No sanity checks at all right now.
   * @param tupleBatch the tupleBatch to be inserted
   */
  public static void tupleBatchInsert(String driverClassName, String connectionString,
      String insertString, TupleBatch tupleBatch) {
    try {
      /* Extract the Schema */
      Schema schema = tupleBatch.getSchema();

      /* Make sure JDBC driver is loaded */
      Class.forName(driverClassName);
      /* Connect to the database */
      Connection jdbcConnection = DriverManager.getConnection(connectionString);
      /* Set read only on the connection */
      jdbcConnection.setReadOnly(true);

      /* Set up and execute the query */
      PreparedStatement statement = jdbcConnection.prepareStatement(insertString);

      Type[] types = schema.getTypes();

      for (int row = 0; row < tupleBatch.numTuples(); ++row) {
        for (int column = 0; column < types.length; ++column) {
          if (types[column] == Type.DOUBLE_TYPE) {
            statement.setDouble(column + 1, tupleBatch.getDouble(column, row));
          } else if (types[column] == Type.FLOAT_TYPE) {
            statement.setFloat(column + 1, tupleBatch.getFloat(column, row));
          } else if (types[column] == Type.INT_TYPE) {
            statement.setInt(column + 1, tupleBatch.getInt(column, row));
          } else if (types[column] == Type.BOOLEAN_TYPE) {
            statement.setBoolean(column + 1, tupleBatch.getBoolean(column, row));
          } else if (types[column] == Type.STRING_TYPE) {
            statement.setString(column + 1, tupleBatch.getString(column, row));
          } else {
            throw new RuntimeException("Unexpected type: " + types[column].toString());
          }
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
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(String driverClassName,
      String connectionString, String queryString) {
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
  private ResultSet resultSet;
  private Schema schema;

  JdbcTupleBatchIterator(ResultSet resultSet, Schema schema) {
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
          resultSet.getStatement().close(); /*
                                             * Also closes the resultSet
                                             */
          connection.close();
          break;
        }
        for (int fieldIndex = 0; fieldIndex < numFields; ++fieldIndex) {
          if (fieldTypes[fieldIndex] == Type.INT_TYPE) {
            /* JDBC is 1-indexed */
            ((IntColumn) columns.get(fieldIndex)).putInt(resultSet.getInt(fieldIndex + 1));
          } else if (fieldTypes[fieldIndex] == Type.STRING_TYPE) {
            /* JDBC is 1-indexed */
            ((StringColumn) columns.get(fieldIndex)).putString(resultSet.getString(fieldIndex + 1));
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