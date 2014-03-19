package edu.washington.escience.myria.accessmethod;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.CsvTupleWriter;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;

/**
 * Access method for a JDBC database. Exposes data as TupleBatches.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcAccessMethod extends AccessMethod {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcAccessMethod.class);
  /** The database connection information. */
  private JdbcInfo jdbcInfo;
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
    this.jdbcInfo = jdbcInfo;
    connect(jdbcInfo, readOnly);
  }

  /**
   * @return the jdbc connection.
   */
  public Connection getConnection() {
    return jdbcConnection;
  }

  @Override
  public void connect(final ConnectionInfo connectionInfo, final Boolean readOnly) throws DbException {
    Objects.requireNonNull(connectionInfo);

    jdbcConnection = null;
    jdbcInfo = (JdbcInfo) connectionInfo;
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
  public void tupleBatchInsert(final RelationKey relationKey, final Schema schema, final TupleBatch tupleBatch)
      throws DbException {
    LOGGER.debug("Inserting batch of size {}", tupleBatch.numTuples());
    Objects.requireNonNull(jdbcConnection);
    if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
      // Use the postgres COPY command which is much faster
      try {
        CopyManager cpManager = ((PGConnection) jdbcConnection).getCopyAPI();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TupleWriter tw = new CsvTupleWriter(',', baos);
        tw.writeTuples(tupleBatch);
        tw.done();

        Reader reader = new InputStreamReader(new ByteArrayInputStream(baos.toByteArray()));
        long inserted =
            cpManager.copyIn("COPY " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)
                + " FROM STDIN WITH CSV", reader);
        Preconditions.checkState(inserted == tupleBatch.numTuples(),
            "Error: inserted a batch of size %s but only actually inserted %s rows", tupleBatch.numTuples(), inserted);
      } catch (final SQLException | IOException e) {
        LOGGER.error(e.getMessage(), e);
        throw new DbException(e);
      }
    } else {
      try {
        /* Set up and execute the query */
        final PreparedStatement statement =
            jdbcConnection.prepareStatement(insertStatementFromSchema(schema, relationKey));
        tupleBatch.getIntoJdbc(statement);
        // TODO make it also independent. should be getIntoJdbc(statement,
        // tupleBatch)
        statement.executeBatch();
        statement.close();
      } catch (final SQLException e) {
        LOGGER.error(e.getMessage(), e);
        throw new DbException(e);
      }
    }
    LOGGER.debug(".. done inserting batch of size {}", tupleBatch.numTuples());
  }

  @Override
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString, final Schema schema)
      throws DbException {
    Objects.requireNonNull(jdbcConnection);
    try {
      Statement statement;
      if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
        /*
         * Special handling for PostgreSQL comes from here:
         * http://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
         */
        jdbcConnection.setAutoCommit(false);
        statement = jdbcConnection.createStatement();
        statement.setFetchSize(TupleBatch.BATCH_SIZE);
      } else if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_MYSQL)) {
        /*
         * Special handling for MySQL comes from here:
         * http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
         */
        statement =
            jdbcConnection.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
      } else {
        /* Unknown tricks for this DBMS. Hope it works! */
        statement = jdbcConnection.createStatement();
        statement.setFetchSize(TupleBatch.BATCH_SIZE);
      }
      final ResultSet resultSet = statement.executeQuery(queryString);
      return new JdbcTupleBatchIterator(resultSet, schema);
    } catch (final SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e);
    }
  }

  @Override
  public void close() throws DbException {
    /* Close the db connection. */
    try {
      jdbcConnection.close();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e);
    }
  }

  @Override
  public void init() throws DbException {
  }

  @Override
  public void execute(final String ddlCommand) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    LOGGER.debug("Executing command {}", ddlCommand);
    Statement statement;
    try {
      statement = jdbcConnection.createStatement();
      statement.execute(ddlCommand);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DbException(e);
    }
  }

  /**
   * Insert the Tuples in this TupleBatch into the database.
   * 
   * @param jdbcInfo information about the connection parameters.
   * @param relationKey the table to insert into.
   * @param schema the schema of the relation.
   * @param tupleBatch the tupleBatch to be inserted.
   * @throws DbException if there is an error inserting these tuples.
   */
  public static void tupleBatchInsert(final JdbcInfo jdbcInfo, final RelationKey relationKey, final Schema schema,
      final TupleBatch tupleBatch) throws DbException {
    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, false);
    jdbcAccessMethod.tupleBatchInsert(relationKey, schema, tupleBatch);
    jdbcAccessMethod.close();
  }

  /**
   * Create a JDBC Connection and then expose the results as an Iterator<TupleBatch>.
   * 
   * @param jdbcInfo the JDBC connection information.
   * @param queryString the query.
   * @param schema the schema of the returned tuples.
   * @return an Iterator<TupleBatch> containing the results.
   * @throws DbException if there is an error getting tuples.
   */
  public static Iterator<TupleBatch> tupleBatchIteratorFromQuery(final JdbcInfo jdbcInfo, final String queryString,
      final Schema schema) throws DbException {
    JdbcAccessMethod jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, true);
    return jdbcAccessMethod.tupleBatchIteratorFromQuery(queryString, schema);
  }

  @Override
  public void createTableIfNotExists(final RelationKey relationKey, final Schema schema) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    Objects.requireNonNull(jdbcInfo);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);

    execute(createIfNotExistsStatementFromSchema(schema, relationKey));
  }

  @Override
  public String insertStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(relationKey);
    final StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(relationKey.toString(jdbcInfo.getDbms())).append(" (");
    sb.append(StringUtils.join(schema.getColumnNames(), ','));
    sb.append(") VALUES (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('?');
    }
    sb.append(");");
    return sb.toString();
  }

  @Override
  public String createIfNotExistsStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    switch (jdbcInfo.getDbms()) {
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
        /* This function is supported for Postgres and Mysql. */
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        throw new UnsupportedOperationException("MonetDB cannot CREATE TABLE IF NOT EXISTS");
      default:
        throw new UnsupportedOperationException("Don't know whether DBMS " + jdbcInfo.getDbms()
            + " can CREATE TABLE IF NOT EXISTS");
    }

    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE IF NOT EXISTS ").append(relationKey.toString(jdbcInfo.getDbms())).append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(schema.getColumnName(i)).append(" ")
          .append(typeToDbmsType(schema.getColumnType(i), jdbcInfo.getDbms()));
    }
    sb.append(");");
    return sb.toString();
  }

  /**
   * Helper utility for creating JDBC CREATE TABLE statements.
   * 
   * @param type a Myria column type.
   * @param dbms the description of the DBMS, e.g., "mysql".
   * @return the name of the DBMS type that matches the given Myria type.
   */
  public static String typeToDbmsType(final Type type, final String dbms) {
    switch (type) {
      case BOOLEAN_TYPE:
        return "BOOLEAN";
      case DOUBLE_TYPE:
      case FLOAT_TYPE:
        switch (dbms) {
          case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
            return "DOUBLE PRECISION";
          default:
            return "DOUBLE";
        }
      case INT_TYPE:
        return "INTEGER";
      case LONG_TYPE:
        return "BIGINT";
      case STRING_TYPE:
        return "TEXT";
      case DATETIME_TYPE:
        return "TIMESTAMP";
      default:
        throw new UnsupportedOperationException("Type " + type + " is not supported by DBMS " + dbms);
    }
  }

  @Override
  public void dropAndRenameTables(final RelationKey oldRelation, final RelationKey newRelation) throws DbException {
    Objects.requireNonNull(oldRelation);
    Objects.requireNonNull(newRelation);
    final String oldName = oldRelation.toString(jdbcInfo.getDbms());
    final String newName = newRelation.toString(jdbcInfo.getDbms());

    switch (jdbcInfo.getDbms()) {
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        dropTableIfExists(oldRelation);
        execute("RENAME TABLE " + newName + " TO " + oldName);
        break;
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
        dropTableIfExists(oldRelation);
        execute("ALTER TABLE " + newName + " RENAME TO " + oldName);
        break;
      default:
        throw new UnsupportedOperationException("Don't know how to rename tables for DBMS " + jdbcInfo.getDbms());
    }
  }

  @Override
  public void dropTableIfExists(final RelationKey relationKey) throws DbException {
    switch (jdbcInfo.getDbms()) {
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        execute("DROP TABLE IF EXISTS " + relationKey.toString(jdbcInfo.getDbms()));
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        throw new UnsupportedOperationException("MonetDB cannot DROP IF EXISTS tables");
      default:
        throw new UnsupportedOperationException("Don't know whether " + jdbcInfo.getDbms()
            + " can DROP IF EXISTS a table");
    }
  }

  /**
   * @param relationKey the relation to be indexed.
   * @param index the list of columns in the index.
   * @return the canonical RelationKey of the index of that table on that column.
   */
  private RelationKey getIndexName(final RelationKey relationKey, final List<IndexRef> index) {
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(index);

    /* All indexes go in a separate "program" that has the name "__myria_indexes" appended to it. */
    StringBuilder name = new StringBuilder(relationKey.getProgramName()).append("__myria_indexes");
    if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_MYSQL)) {
      /* Rename is not supported, append a timestamp. */
      name.append('_').append(System.nanoTime());
    }
    final String indexProgramName = name.toString();

    /* Build the relation name for the index. */
    name = new StringBuilder(relationKey.getRelationName());
    for (IndexRef i : index) {
      Objects.requireNonNull(i);
      name.append('_').append(i.getColumn());
      if (!i.isAscending()) {
        name.append('D');
      }
    }

    RelationKey indexRelationKey = RelationKey.of(relationKey.getUserName(), indexProgramName, name.toString());
    return indexRelationKey;
  }

  /**
   * @param schema the schema of the relation to be indexed.
   * @param index the list of columns to be indexed.
   * @return the string defining the index, e.g., "(col1, col2, col3)".
   */
  private String getIndexColumns(final Schema schema, final List<IndexRef> index) {
    Objects.requireNonNull(schema);
    Objects.requireNonNull(index);

    StringBuilder columns = new StringBuilder("(");
    boolean first = true;
    for (IndexRef i : index) {
      Objects.requireNonNull(i);
      Preconditions.checkElementIndex(i.getColumn(), schema.numColumns());
      if (!first) {
        columns.append(',');
      }
      first = false;
      columns.append(schema.getColumnName(i.getColumn()));
      if (i.isAscending()) {
        columns.append(" ASC");
      } else {
        columns.append(" DESC");
      }
    }
    columns.append(')');

    return columns.toString();
  }

  @Override
  public void createIndexes(final RelationKey relationKey, final Schema schema, final List<List<IndexRef>> indexes)
      throws DbException {
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(schema);
    Objects.requireNonNull(indexes);

    String sourceTableName = relationKey.toString(jdbcInfo.getDbms());

    for (List<IndexRef> index : indexes) {
      String indexName = getIndexName(relationKey, index).toString(jdbcInfo.getDbms());
      String indexColumns = getIndexColumns(schema, index);

      StringBuilder statement = new StringBuilder("CREATE INDEX ");
      statement.append(indexName).append(" ON ").append(sourceTableName).append(indexColumns);

      execute(statement.toString());
    }
  }

  @Override
  public void renameIndexes(final RelationKey oldRelation, final RelationKey newRelation,
      final List<List<IndexRef>> indexes) throws DbException {

    if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_MYSQL)) {
      /* Do nothing -- rather than renaming the right way, we create every index with a different unique name. */
      return;
    }

    for (List<IndexRef> index : indexes) {
      String oldName = getIndexName(oldRelation, index).toString(jdbcInfo.getDbms());
      String newName = getIndexName(newRelation, index).toString(jdbcInfo.getDbms());
      StringBuilder statement = new StringBuilder("ALTER INDEX ").append(oldName).append(" RENAME TO ").append(newName);
      execute(statement.toString());
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
  /** The results from a JDBC query that will be returned in TupleBatches by this Iterator. */
  private final ResultSet resultSet;
  /** The Schema of the TupleBatches returned by this Iterator. */
  private final Schema schema;
  /** Next TB. */
  private TupleBatch nextTB = null;
  /** statement is closed or not. */
  private boolean statementClosed = false;

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
    if (nextTB != null) {
      return true;
    } else {
      try {
        nextTB = getNextTB();
        return null != nextTB;
      } catch (final SQLException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @return next TupleBatch, null if no more
   * @throws SQLException if any DB system errors
   * */
  private TupleBatch getNextTB() throws SQLException {
    if (statementClosed) {
      return null;
    }
    final int numFields = schema.numColumns();
    final List<ColumnBuilder<?>> columnBuilders = ColumnFactory.allocateColumns(schema);
    int numTuples = 0;
    for (numTuples = 0; numTuples < TupleBatch.BATCH_SIZE; ++numTuples) {
      if (!resultSet.next()) {
        final Connection connection = resultSet.getStatement().getConnection();
        resultSet.getStatement().close();
        connection.close(); /* Also closes the resultSet */
        statementClosed = true;
        break;
      }
      for (int colIdx = 0; colIdx < numFields; ++colIdx) {
        /* Warning: JDBC is 1-indexed */
        columnBuilders.get(colIdx).appendFromJdbc(resultSet, colIdx + 1);
      }
    }
    if (numTuples > 0) {
      List<Column<?>> columns = new ArrayList<Column<?>>(columnBuilders.size());
      for (ColumnBuilder<?> cb : columnBuilders) {
        columns.add(cb.build());
      }

      return new TupleBatch(schema, columns, numTuples);
    } else {
      return null;
    }
  }

  @Override
  public TupleBatch next() {
    TupleBatch tmp = nextTB;
    nextTB = null;
    return tmp;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("JdbcTupleBatchIterator.remove()");
  }
}
