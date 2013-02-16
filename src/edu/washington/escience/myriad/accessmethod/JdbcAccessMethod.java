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

import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;

/**
 * Access method for a JDBC database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcAccessMethod {

  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class.getName());

  /**
   * Insert the Tuples in this TupleBatch into the database. Unlike the other tupleBatchInsert method, this does not
   * open a connection to the database but uses the one it is passed.
   * 
   * @param jdbcConnection the connection to the database
   * @param insertString the insert statement. TODO no sanity checks at all right now
   * @param tupleBatch the tupleBatch to be inserted
   */
  public static void tupleBatchInsert(final Connection jdbcConnection, final String insertString,
      final TupleBatch tupleBatch) {

    if (jdbcConnection == null) {
      throw new IllegalArgumentException();
    }

    try {
      /* Set up and execute the query */
      final PreparedStatement statement = jdbcConnection.prepareStatement(insertString);

      tupleBatch.getIntoJdbc(statement);
      statement.executeBatch();
      statement.close();
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Insert the Tuples in this TupleBatch into the database.
   * 
   * @param driverClassName the JDBC driver name
   * @param connectionString the string identifying the path to the database
   * @param insertString the insert statement. TODO No sanity checks at all right now.
   * @param tupleBatch the tupleBatch to be inserted.
   * @param username the user by which to connect to the SQL database.
   * @param password the password for the SQL user identified by username.
   */
  public static void tupleBatchInsert(final String driverClassName, final String connectionString,
      final String insertString, final TupleBatch tupleBatch, final String username, final String password) {
    try {
      /* Make sure JDBC driver is loaded */
      Class.forName(driverClassName);
      /* Connect to the database */
      final Connection jdbcConnection = DriverManager.getConnection(connectionString, username, password);

      tupleBatchInsert(jdbcConnection, insertString, tupleBatch);

    } catch (final ClassNotFoundException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * Create a JDBC Connection and then expose the results as an Iterator<TupleBatch>.
   * 
   * @param driverClassName the JDBC driver name
   * @param connectionString the string identifying the path to the database
   * @param queryString the query
   * @param username the user by which to connect to the SQL database.
   * @param password the password for the SQL user identified by username.
   * @return an Iterator<TupleBatch> containing the results.
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String driverClassName,
      final String connectionString, final String queryString, final String username, final String password) {
    try {
      /* Make sure JDBC driver is loaded */
      Class.forName(driverClassName);
      /* Connect to the database */
      final Connection jdbcConnection = DriverManager.getConnection(connectionString, username, password);
      /* Set read only on the connection */
      jdbcConnection.setReadOnly(true);

      /* Set up and execute the query */
      final Statement statement = jdbcConnection.createStatement();
      final ResultSet resultSet = statement.executeQuery(queryString);
      final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      return new JdbcTupleBatchIterator(resultSet, Schema.fromResultSetMetaData(resultSetMetaData));
    } catch (final ClassNotFoundException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e);
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage());
      throw new RuntimeException(e);
    }
  }

  /** Inaccessible. */
  private JdbcAccessMethod() {
    throw new AssertionError();
  }
}

/**
 * Wraps a JDBC ResultSet in a Iterator<TupleBatch>.
 * 
 * Implementation based on org.apache.commons.dbutils.ResultSetIterator. Requires ResultSet.isLast() to be implemented.
 * 
 * @author dhalperi
 * 
 */
class JdbcTupleBatchIterator implements Iterator<TupleBatch> {
  /** The logger for this class, uses JdbcAccessMethod settings. */
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class.getName());
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
    } catch (final SQLException e) {
      LOGGER.error("Dropping SQLException:" + e);
      return false;
    }
  }

  @Override
  public TupleBatch next() {
    /* Allocate TupleBatch parameters */
    final int numFields = schema.numColumns();
    final List<Column<?>> columns = ColumnFactory.allocateColumns(schema);

    /**
     * Loop through resultSet, adding one row at a time. Stop when numTuples hits BATCH_SIZE or there are no more
     * results.
     */
    int numTuples;
    try {
      for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE; ++numTuples) {
        if (!resultSet.next()) {
          final Connection connection = resultSet.getStatement().getConnection();
          resultSet.getStatement().close();
          connection.close(); /* Also closes the resultSet */
          break;
        }
        for (int colIdx = 0; colIdx < numFields; ++colIdx) {
          /* Warning: JDBC is 1-indexed */
          columns.get(colIdx).putFromJdbc(resultSet, colIdx + 1);
        }
      }
    } catch (final SQLException e) {
      LOGGER.error("Got SQLException:" + e + "in JdbcTupleBatchIterator.next()");
      throw new RuntimeException(e.getMessage());
    }

    return new TupleBatch(schema, columns, numTuples);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("JdbcTupleBatchIterator.remove()");
  }
}
