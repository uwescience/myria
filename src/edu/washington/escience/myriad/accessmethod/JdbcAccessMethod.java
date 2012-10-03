package edu.washington.escience.myriad.accessmethod;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;

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
    Connection jdbcConnection = JdbcConnectionManager.getConnection(driverClassName, connectionString);
    try {
      /* Set up and execute the query */
      PreparedStatement statement = jdbcConnection.prepareStatement(insertString);

      for (int row : tupleBatch.validTupleIndices()) {
        for (int column = 0; column < tupleBatch.numColumns(); ++column) {
          tupleBatch.getColumn(column).getIntoJdbc(row, statement, column + 1);
        }
        statement.addBatch();
      }
      statement.executeBatch();
      statement.close();
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
    Connection jdbcConnection = JdbcConnectionManager.getConnection(driverClassName, connectionString);
    try {
      /* Set up and execute the query */
      Statement statement = jdbcConnection.createStatement();
      ResultSet resultSet = statement.executeQuery(queryString);
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      return new JdbcTupleBatchIterator(resultSet, Schema.fromResultSetMetaData(resultSetMetaData));
    } catch (SQLException e) {
      System.err.println(e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  /** Inaccessible. */
  private JdbcAccessMethod() {
    throw new AssertionError();
  }
}

/**
 * JdbcConnectionManager reuses existing connections to JDBC databases if possible.
 * 
 * @author dhalperi
 * 
 */
final class JdbcConnectionManager {
  /** A cache of the connections that have been made. */
  private static HashMap<Integer, Connection> connections;
  /** A cache of the JDBC drivers that have been loaded. */
  private static HashSet<Integer> loadedDrivers;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE1 = 247;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE2 = 89;

  /**
   * Gets a JDBC Connection object using the given driver name and connection string. This implementation will reuse
   * connection objects when they succeed. This function returns null if the driver cannot be loaded or there is an
   * error in making the connection.
   * 
   * @param driverClassName the JDBC driver name
   * @param connectionString the string identifying the path to the database
   * @return a connection to the requested database, or null during an exception
   */
  static Connection getConnection(final String driverClassName, final String connectionString) {
    HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);

    /* Expected invariant: connections and loadedDrivers are both null or both allocated */
    if (connections == null) {
      connections = new HashMap<Integer, Connection>();
      loadedDrivers = new HashSet<Integer>();
    }

    /* Make sure JDBC driver is loaded */
    int driverHash = hb.append(driverClassName).toHashCode();
    if (!loadedDrivers.contains(driverHash)) {
      try {
        Class.forName(driverClassName);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        return null;
      }
      loadedDrivers.add(driverHash);
    }

    /* Check whether Connection has been made before, use same connection if exists */
    int connectionHash = hb.append(connectionString).toHashCode();
    Connection ret = connections.get(connectionHash);
    if (ret != null) {
      return ret;
    }

    /* Make new connection */
    Connection jdbcConnection;
    try {
      jdbcConnection = DriverManager.getConnection(connectionString);
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }

    /* Store the connection and return it */
    connections.put(connectionHash, jdbcConnection);
    return jdbcConnection;
  }

  /** Inaccessible. */
  private JdbcConnectionManager() {
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
    List<Column> columns = ColumnFactory.allocateColumns(schema);

    /**
     * Loop through resultSet, adding one row at a time. Stop when numTuples hits BATCH_SIZE or there are no more
     * results.
     */
    int numTuples;
    try {
      for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE; ++numTuples) {
        if (!resultSet.next()) {
          resultSet.getStatement().close();
          resultSet.close();
          break;
        }
        for (int colIdx = 0; colIdx < numFields; ++colIdx) {
          /* Warning: JDBC is 1-indexed */
          columns.get(colIdx).putFromJdbc(resultSet, colIdx + 1);
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