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
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.util.JdbcUtils;

/**
 * Access method for a JDBC database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcAccessMethod {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class);

  /**
   * Get a JDBC Connection object for the given parameters.
   * 
   * @param jdbcInfo the information needed to connect to the database.
   * @return the connection.
   * @throws DbException if there is an error making the connection.
   */
  public static Connection getConnection(final JdbcInfo jdbcInfo) throws DbException {
    Objects.requireNonNull(jdbcInfo);

    try {
      DriverManager.setLoginTimeout(5);
      /* Make sure JDBC driver is loaded */
      Class.forName(jdbcInfo.getDriverClass());
      return DriverManager.getConnection(jdbcInfo.getConnectionString(), jdbcInfo.getProperties());
    } catch (ClassNotFoundException | SQLException e) {
      throw new DbException(e);
    }
  }

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
    Objects.requireNonNull(jdbcConnection);

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
   * @param jdbcInfo information about the connection parameters.
   * @param insertString the insert statement. TODO No sanity checks at all right now.
   * @param tupleBatch the tupleBatch to be inserted.
   * @throws DbException if there is an error inserting these tuples.
   */
  public static void tupleBatchInsert(final JdbcInfo jdbcInfo, final String insertString, final TupleBatch tupleBatch)
      throws DbException {
    /* Connect to the database. */
    final Connection jdbcConnection = getConnection(jdbcInfo);
    /* Insert the tuples. */
    tupleBatchInsert(jdbcConnection, insertString, tupleBatch);
    /* Close the db connection. */
    try {
      jdbcConnection.close();
    } catch (SQLException e) {
      throw new DbException(e);
    }
  }

  /**
   * Create a JDBC Connection and then expose the results as an Iterator<TupleBatch>.
   * 
   * @param jdbcInfo the JDBC connection information.
   * @param queryString the query
   * @return an Iterator<TupleBatch> containing the results.
   * @throws DbException if there is an error getting tuples.
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final JdbcInfo jdbcInfo, final String queryString)
      throws DbException {
    try {
      /* Connect to the database */
      final Connection jdbcConnection = getConnection(jdbcInfo);
      /* Set read only on the connection */
      jdbcConnection.setReadOnly(true);

      /* Set up and execute the query */
      final Statement statement = jdbcConnection.createStatement();
      final ResultSet resultSet = statement.executeQuery(queryString);
      final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      return new JdbcTupleBatchIterator(resultSet, Schema.fromResultSetMetaData(resultSetMetaData));
    } catch (final SQLException e) {
      throw new DbException(e);
    }
  }

  /** Inaccessible. */
  private JdbcAccessMethod() {
    throw new AssertionError();
  }

  /**
   * Create a table with the given name and schema in the database. If dropExisting is true, drops an existing table if
   * it exists.
   * 
   * @param connection the JDBC connection to the database.
   * @param relationKey the name of the relation.
   * @param schema the schema of the relation.
   * @param dbms the DBMS, e.g., "mysql".
   * @param dropExisting if true, an existing relation will be dropped.
   * @throws DbException if there is an error in the database.
   */
  public static void createTable(final Connection connection, final RelationKey relationKey, final Schema schema,
      final String dbms, final boolean dropExisting) throws DbException {
    Objects.requireNonNull(connection);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);
    Statement statement;
    try {
      statement = connection.createStatement();
    } catch (SQLException e) {
      throw new DbException(e);
    }
    try {
      statement.execute("DROP TABLE " + relationKey.toString(dbms) + ";");
    } catch (SQLException e) {
      ; /* Skip. this is okay. */
    }
    try {
      statement.execute(JdbcUtils.createStatementFromSchema(schema, relationKey, dbms));
    } catch (SQLException e) {
      throw new DbException(e);
    }
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
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class);
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
