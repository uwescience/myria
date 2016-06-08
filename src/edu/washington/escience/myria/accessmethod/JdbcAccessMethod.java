package edu.washington.escience.myria.accessmethod;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.PGStatement;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.PostgresBinaryTupleWriter;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleWriter;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.ErrorUtils;

/**
 * Access method for a JDBC database. Exposes data as TupleBatches.
 *
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
    Objects.requireNonNull(jdbcInfo, "jdbcInfo");
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
  public void connect(final ConnectionInfo connectionInfo, final Boolean readOnly)
      throws DbException {
    Objects.requireNonNull(connectionInfo, "connectionInfo");

    jdbcConnection = null;
    jdbcInfo = (JdbcInfo) connectionInfo;
    try {
      DriverManager.setLoginTimeout(5);
      /* Make sure JDBC driver is loaded */
      Class.forName(jdbcInfo.getDriverClass());
      jdbcConnection =
          DriverManager.getConnection(jdbcInfo.getConnectionString(), jdbcInfo.getProperties());
    } catch (ClassNotFoundException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    } catch (SQLException e) {
      throw ErrorUtils.mergeSQLException(e);
    }
  }

  @Override
  public void setReadOnly(final Boolean readOnly) throws DbException {
    Objects.requireNonNull(jdbcConnection, "jdbcConnection");

    try {
      if (jdbcConnection.isReadOnly() != readOnly) {
        jdbcConnection.setReadOnly(readOnly);
      }
    } catch (SQLException e) {
      throw ErrorUtils.mergeSQLException(e);
    }
  }

  /**
   * Helper function to copy data into PostgreSQL using the COPY command.
   *
   * @param relationKey the destination relation
   * @param schema the schema of the relation
   * @param tupleBatch the tuples to be inserted.
   * @throws DbException if there is an error.
   */
  private void postgresCopyInsert(
      final RelationKey relationKey, final Schema schema, final TupleBatch tupleBatch)
      throws DbException {
    // Use the postgres COPY command which is much faster
    try {
      CopyManager cpManager = ((PGConnection) jdbcConnection).getCopyAPI();
      StringBuilder copyString =
          new StringBuilder()
              .append("COPY ")
              .append(quote(relationKey))
              .append(" FROM STDIN WITH BINARY");
      CopyIn copyIn = cpManager.copyIn(copyString.toString());

      TupleWriter tw = new PostgresBinaryTupleWriter();
      tw.open(new PGCopyOutputStream(copyIn));
      tw.writeTuples(tupleBatch);
      tw.done();

      long inserted = copyIn.getHandledRowCount();
      Preconditions.checkState(
          inserted == tupleBatch.numTuples(),
          "Error: inserted a batch of size %s but only actually inserted %s rows",
          tupleBatch.numTuples(),
          inserted);
    } catch (final SQLException e) {
      throw ErrorUtils.mergeSQLException(e);
    } catch (final IOException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void tupleBatchInsert(final RelationKey relationKey, final TupleBatch tupleBatch)
      throws DbException {
    LOGGER.debug("Inserting batch of size {}", tupleBatch.numTuples());
    Objects.requireNonNull(jdbcConnection, "jdbcConnection");

    Schema schema = tupleBatch.getSchema();

    boolean writeSucceeds = false;
    if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
      /*
       * There are bugs when using the COPY command to store doubles and floats into PostgreSQL. See
       * uwescience/myria-web#48
       */
      try {
        postgresCopyInsert(relationKey, schema, tupleBatch);
        writeSucceeds = true;
      } catch (DbException e) {
        LOGGER.error("Error inserting batch via PostgreSQL COPY", e);
        /*
         * TODO - should we do a VACUUM now? The bad rows will not be visible to the DB, however, so the write did not
         * partially happen.
         *
         * http://www.postgresql.org/docs/9.2/static/sql-copy.html
         */
      }
    }
    if (!writeSucceeds) {
      try {
        /* Set up and execute the query */
        final PreparedStatement statement =
            jdbcConnection.prepareStatement(insertStatementFromSchema(schema, relationKey));
        for (int row = 0; row < tupleBatch.numTuples(); ++row) {
          for (int col = 0; col < tupleBatch.numColumns(); ++col) {
            switch (schema.getColumnType(col)) {
              case BOOLEAN_TYPE:
                statement.setBoolean(col + 1, tupleBatch.getBoolean(col, row));
                break;
              case DATETIME_TYPE:
                statement.setTimestamp(
                    col + 1, new Timestamp(tupleBatch.getDateTime(col, row).getMillis()));
                break;
              case DOUBLE_TYPE:
                statement.setDouble(col + 1, tupleBatch.getDouble(col, row));
                break;
              case FLOAT_TYPE:
                statement.setFloat(col + 1, tupleBatch.getFloat(col, row));
                break;
              case INT_TYPE:
                statement.setInt(col + 1, tupleBatch.getInt(col, row));
                break;
              case LONG_TYPE:
                statement.setLong(col + 1, tupleBatch.getLong(col, row));
                break;
              case STRING_TYPE:
                statement.setString(col + 1, tupleBatch.getString(col, row));
                break;
            }
          }
          statement.addBatch();
        }
        statement.executeBatch();
        statement.close();
      } catch (final SQLException e) {
        throw ErrorUtils.mergeSQLException(e);
      }
    }
    LOGGER.debug(".. done inserting batch of size {}", tupleBatch.numTuples());
  }

  @Override
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(
      final String queryString, final Schema schema) throws DbException {
    Objects.requireNonNull(jdbcConnection, "jdbcConnection");
    try {
      PreparedStatement statement;
      if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
        /*
         * Special handling for PostgreSQL comes from here:
         * http://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
         */
        jdbcConnection.setAutoCommit(false);
        statement = jdbcConnection.prepareStatement(queryString);
        ((PGStatement) statement).setPrepareThreshold(-1);
        statement.setFetchSize(TupleBatch.BATCH_SIZE);
      } else if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_MYSQL)) {
        /*
         * Special handling for MySQL comes from here:
         * http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
         */
        statement =
            jdbcConnection.prepareStatement(
                queryString,
                java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE);
      } else {
        /* Unknown tricks for this DBMS. Hope it works! */
        statement = jdbcConnection.prepareStatement(queryString);
        statement.setFetchSize(TupleBatch.BATCH_SIZE);
      }
      final ResultSet resultSet = statement.executeQuery();
      return new JdbcTupleBatchIterator(resultSet, schema);
    } catch (final SQLException e) {
      throw ErrorUtils.mergeSQLException(e);
    }
  }

  @Override
  public void close() throws DbException {
    /* Close the db connection. */
    try {
      jdbcConnection.close();
    } catch (SQLException e) {
      throw ErrorUtils.mergeSQLException(e);
    }
  }

  @Override
  public void init() throws DbException {}

  @Override
  public void execute(final String ddlCommand) throws DbException {
    Objects.requireNonNull(jdbcConnection);
    LOGGER.debug("Executing command {}", ddlCommand);
    Statement statement;
    try {
      statement = jdbcConnection.createStatement();
      statement.execute(ddlCommand);
    } catch (SQLException e) {
      throw ErrorUtils.mergeSQLException(e);
    }
  }

  @Override
  public void createTableIfNotExists(final RelationKey relationKey, final Schema schema)
      throws DbException {
    Objects.requireNonNull(jdbcConnection, "jdbcConnection");
    Objects.requireNonNull(jdbcInfo, "jdbcInfo");
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(schema, "schema");

    execute(createIfNotExistsStatementFromSchema(schema, relationKey));
  }

  /**
   * Create an unlogged table.
   *
   * @param relationKey the relation name
   * @param schema the relation schema
   * @throws DbException if anything goes wrong
   */
  public void createUnloggedTableIfNotExists(final RelationKey relationKey, final Schema schema)
      throws DbException {
    Objects.requireNonNull(jdbcConnection, "jdbcConnection");
    Objects.requireNonNull(jdbcInfo, "jdbcInfo");
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(schema, "schema");

    execute(createIfNotExistsStatementFromSchema(schema, relationKey, true));
  }

  @Override
  public String insertStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(schema, "schema");
    final StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ").append(quote(relationKey)).append(" (");
    sb.append(StringUtils.join(quotedColumnNames(schema), ','));
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

  /**
   * Returns a list of the column names in the given schema, quoted.
   *
   * @param schema the relation whose columns to quote.
   * @return a list of the column names in the given schema, quoted.
   */
  private List<String> quotedColumnNames(final Schema schema) {
    ImmutableList.Builder<String> b = ImmutableList.builder();
    for (String col : schema.getColumnNames()) {
      b.add(quote(col));
    }
    return b.build();
  }

  @Override
  public String createIfNotExistsStatementFromSchema(
      final Schema schema, final RelationKey relationKey) {
    return createIfNotExistsStatementFromSchema(schema, relationKey, false);
  }

  /**
   * @param schema the relation schema
   * @param relationKey the relation name
   * @param unlogged whether to create an unlogged table
   * @return the insert statement string
   */
  private String createIfNotExistsStatementFromSchema(
      final Schema schema, final RelationKey relationKey, final boolean unlogged) {
    switch (jdbcInfo.getDbms()) {
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
        /* This function is supported for PostgreSQL and MySQL. */
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        throw new UnsupportedOperationException("MonetDB cannot CREATE TABLE IF NOT EXISTS");
      default:
        throw new UnsupportedOperationException(
            "Don't know whether DBMS " + jdbcInfo.getDbms() + " can CREATE TABLE IF NOT EXISTS");
    }

    Preconditions.checkArgument(
        !unlogged || jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL),
        "Can only create unlogged table in Postgres");
    String unloggedString = "";
    if (unlogged) {
      unloggedString = "UNLOGGED ";
    }

    final StringBuilder sb = new StringBuilder();
    sb.append("CREATE ")
        .append(unloggedString)
        .append("TABLE IF NOT EXISTS ")
        .append(quote(relationKey))
        .append(" (");
    for (int i = 0; i < schema.numColumns(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(quote(schema.getColumnName(i)))
          .append(" ")
          .append(typeToDbmsType(schema.getColumnType(i), jdbcInfo.getDbms()))
          .append(" NOT NULL");
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
        switch (dbms) {
          case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
            return "DOUBLE PRECISION";
          default:
            return "DOUBLE";
        }
      case FLOAT_TYPE:
        switch (dbms) {
          case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
            return "REAL";
          default:
            return "FLOAT";
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
        throw new UnsupportedOperationException(
            "Type " + type + " is not supported by DBMS " + dbms);
    }
  }

  @Override
  public void dropAndRenameTables(final RelationKey oldRelation, final RelationKey newRelation)
      throws DbException {
    Objects.requireNonNull(oldRelation, "oldRelation");
    Objects.requireNonNull(newRelation, "newRelation");
    final String oldName = quote(oldRelation);
    final String newName = quote(newRelation);

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
        throw new UnsupportedOperationException(
            "Don't know how to rename tables for DBMS " + jdbcInfo.getDbms());
    }
  }

  @Override
  public void dropTableIfExistsCascade(final RelationKey relationKey) throws DbException {
    switch (jdbcInfo.getDbms()) {
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        LOGGER.warn("MySQL does not implement DROP TABLE...CASCADE, attempting DROP TABLE instead");
        execute("DROP TABLE IF EXISTS " + quote(relationKey));
        break;
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
        execute("DROP TABLE IF EXISTS " + quote(relationKey) + " CASCADE");
        break;
      default:
        throw new UnsupportedOperationException(
            "Don't know whether " + jdbcInfo.getDbms() + " can DROP IF EXISTS...CASCADE a table");
    }
  }

  @Override
  public void dropTableIfExists(final RelationKey relationKey) throws DbException {
    switch (jdbcInfo.getDbms()) {
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        execute("DROP TABLE IF EXISTS " + quote(relationKey));
        break;
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        throw new UnsupportedOperationException("MonetDB cannot DROP IF EXISTS tables");
      default:
        throw new UnsupportedOperationException(
            "Don't know whether " + jdbcInfo.getDbms() + " can DROP IF EXISTS a table");
    }
  }

  /**
   * @param relationKey the relation to be indexed.
   * @param index the list of columns in the index.
   * @return the canonical RelationKey of the index of that table on that column.
   */
  private RelationKey getIndexName(final RelationKey relationKey, final List<IndexRef> index) {
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(index, "index");

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
      Objects.requireNonNull(i, "i");
      name.append('_').append(i.getColumn());
      if (!i.isAscending()) {
        name.append('D');
      }
    }

    RelationKey indexRelationKey =
        RelationKey.of(relationKey.getUserName(), indexProgramName, name.toString());
    return indexRelationKey;
  }

  /**
   * @param schema the schema of the relation to be indexed.
   * @param index the list of columns to be indexed.
   * @return the string defining the index, e.g., "(col1, col2, col3)".
   */
  private String getIndexColumns(final Schema schema, final List<IndexRef> index) {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(index, "index");

    StringBuilder columns = new StringBuilder("(");
    boolean first = true;
    for (IndexRef i : index) {
      Objects.requireNonNull(i, "i");
      Preconditions.checkElementIndex(i.getColumn(), schema.numColumns());
      if (!first) {
        columns.append(',');
      }
      first = false;
      columns.append(quote(schema.getColumnName(i.getColumn())));
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
  public void createIndexes(
      final RelationKey relationKey, final Schema schema, final List<List<IndexRef>> indexes)
      throws DbException {
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(indexes, "indexes");

    String sourceTableName = quote(relationKey);
    for (List<IndexRef> index : indexes) {
      String indexName = quote(getIndexName(relationKey, index));
      String indexColumns = getIndexColumns(schema, index);
      StringBuilder statement = new StringBuilder("CREATE INDEX ");
      statement.append(indexName).append(" ON ").append(sourceTableName).append(indexColumns);
      execute(statement.toString());
    }
  }

  @Override
  public void createIndexIfNotExists(
      final RelationKey relationKey, final Schema schema, final List<IndexRef> index)
      throws DbException {
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(index, "index");

    if (jdbcInfo.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL)) {
      createIndexIfNotExistPostgres(relationKey, schema, index);
    } else {
      throw new UnsupportedOperationException(
          "create index if not exists is not supported in "
              + jdbcInfo.getDbms()
              + ", implement me");
    }
  }

  /**
   * Create an index in postgres if no index with the same name already exists.
   *
   * @param relationKey the table on which the indexes will be created.
   * @param schema the Schema of the data in the table.
   * @param index the index to be created; each entry is a list of column indices.
   * @throws DbException if there is an error in the DBMS.
   */
  public void createIndexIfNotExistPostgres(
      final RelationKey relationKey, final Schema schema, final List<IndexRef> index)
      throws DbException {
    Objects.requireNonNull(index, "index");

    String sourceTableName = quote(relationKey);
    String indexName = quote(getIndexName(relationKey, index));
    String indexNameSingleQuote = "'" + indexName.substring(1, indexName.length() - 1) + "'";
    String indexColumns = getIndexColumns(schema, index);

    String statement =
        Joiner.on(' ')
            .join(
                "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relkind = 'i' AND relname=",
                indexNameSingleQuote,
                ") THEN CREATE INDEX",
                indexName,
                "ON",
                sourceTableName,
                indexColumns,
                "; END IF; END$$;");
    execute(statement);
  }

  /**
   * Returns the quoted name of the given relation for use in SQL statements.
   *
   * @param relationKey the relation to be quoted.
   * @return the quoted name of the given relation for use in SQL statements.
   */
  private String quote(final RelationKey relationKey) {
    return relationKey.toString(jdbcInfo.getDbms());
  }

  /**
   * Returns the quoted name of the given column for use in SQL statements.
   *
   * @param column the name of the column to be quoted.
   * @return the quoted name of the given column for use in SQL statements.
   */
  private String quote(final String column) {
    char quote;
    switch (jdbcInfo.getDbms()) {
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        quote = '`';
        break;
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        quote = '\"';
        break;
      default:
        throw new UnsupportedOperationException(
            "Don't know how to quote DBMS " + jdbcInfo.getDbms());
    }
    return new StringBuilder().append(quote).append(column).append(quote).toString();
  }
}

/**
 * Wraps a JDBC ResultSet in a Iterator<TupleBatch>.
 *
 * Implementation based on org.apache.commons.dbutils.ResultSetIterator. Requires ResultSet.isLast() to be implemented.
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
        throw new RuntimeException(ErrorUtils.mergeSQLException(e).getCause());
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
