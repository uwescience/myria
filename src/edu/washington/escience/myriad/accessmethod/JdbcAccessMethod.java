package edu.washington.escience.myriad.accessmethod;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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
import edu.washington.escience.myriad.column.ColumnBuilder;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.util.JdbcUtils;

/**
 * Access method for a JDBC database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcAccessMethod implements AccessMethod {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class);

  /** The database connection. */
  private Connection jdbcConnection;

  /**
   * The constructor. Creates an object and connects with the database
   * 
   * @param jdbcInfo connection information
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  public JdbcAccessMethod(final JdbcInfo jdbcInfo, final Boolean readOnly) throws DbException {
    Objects.requireNonNull(jdbcInfo);
    connect(jdbcInfo, readOnly);
  }

  @Override
  public void connect(final ConnectionInfo connectionInfo, final Boolean readOnly) throws DbException {
    Objects.requireNonNull(connectionInfo);

    jdbcConnection = null;
    JdbcInfo jdbcInfo = (JdbcInfo) connectionInfo;
    try {
      DriverManager.setLoginTimeout(5);
      /* Make sure JDBC driver is loaded */
      Class.forName(jdbcInfo.getDriverClass());
      jdbcConnection = DriverManager.getConnection(jdbcInfo.getConnectionString(), jdbcInfo.getProperties());
    } catch (ClassNotFoundException | SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void setReadOnly(final Boolean readOnly) throws DbException {
    Objects.requireNonNull(jdbcConnection);

    try {
      if (jdbcConnection.isReadOnly() != readOnly) {
        jdbcConnection.setReadOnly(readOnly);
      }
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void tupleBatchInsert(final String insertString, final TupleBatch tupleBatch) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    try {
      /* Set up and execute the query */
      final PreparedStatement statement = jdbcConnection.prepareStatement(insertString);
      tupleBatch.getIntoJdbc(statement);
      // TODO make it also independent. should be getIntoJdbc(statement,
      // tupleBatch)
      statement.executeBatch();
      statement.close();
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    try {
      /* Set up and execute the query */
      final Statement statement = jdbcConnection.createStatement();
      final ResultSet resultSet = statement.executeQuery(queryString);
      final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      return new JdbcTupleBatchIterator(resultSet, Schema.fromResultSetMetaData(resultSetMetaData));
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void close() throws DbException {
    /* Close the db connection. */
    try {
      jdbcConnection.close();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void execute(final String ddlCommand) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    Statement statement;
    try {
      statement = jdbcConnection.createStatement();
      statement.execute(ddlCommand);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
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
    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, false);
    jdbcAccessMethod.tupleBatchInsert(insertString, tupleBatch);
    jdbcAccessMethod.close();
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
    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, true);
    return jdbcAccessMethod.tupleBatchIteratorFromQuery(queryString);
  }

  /**
   * Create a table with the given name and schema in the database. If dropExisting is true, drops an existing table if
   * it exists.
   * 
   * @param relationKey the name of the relation.
   * @param schema the schema of the relation.
   * @param dbms the DBMS, e.g., "mysql".
   * @param dropExisting if true, an existing relation will be dropped.
   * @throws DbException if there is an error in the database.
   */
  public void createTable(final RelationKey relationKey, final Schema schema, final String dbms,
      final boolean dropExisting) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);

    try {
      execute("DROP TABLE " + relationKey.toString(dbms) + ";");
    } catch (DbException e) {
      ; /* Skip. this is okay. */
    }
    execute(JdbcUtils.createStatementFromSchema(schema, relationKey, dbms));
  }

  /**
   * Create a table with the given name and schema in the database. If dropExisting is true, drops an existing table if
   * it exists.
   * 
   * @param jdbcInfo the JDBC connection information.
   * @param relationKey the name of the relation.
   * @param schema the schema of the relation.
   * @param dbms the DBMS, e.g., "mysql".
   * @param dropExisting if true, an existing relation will be dropped.
   * @throws DbException if there is an error in the database.
   */
  public static void createTable(final JdbcInfo jdbcInfo, final RelationKey relationKey, final Schema schema,
      final String dbms, final boolean dropExisting) throws DbException {
    Objects.requireNonNull(jdbcInfo);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);

    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, false);
    try {
      jdbcAccessMethod.execute("DROP TABLE " + relationKey.toString(dbms) + ";");
    } catch (DbException e) {
      ; /* Skip. this is okay. */
    }
    jdbcAccessMethod.execute(JdbcUtils.createStatementFromSchema(schema, relationKey, dbms));
    jdbcAccessMethod.close();
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
    final List<ColumnBuilder<?>> columnBuilders = ColumnFactory.allocateColumns(schema);

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
          columnBuilders.get(colIdx).appendFromJdbc(resultSet, colIdx + 1);
        }
      }
    } catch (final SQLException e) {
      LOGGER.error("Got SQLException:" + e + "in JdbcTupleBatchIterator.next()", e);
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
    throw new UnsupportedOperationException("JdbcTupleBatchIterator.remove()");
  }
}
