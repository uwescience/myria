package edu.washington.escience.myria.operator;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Push a select query down into a JDBC based database and scan over the query result.
 * */
public class DbQueryScan extends LeafOperator implements DbReader {

  /**
   * The connection info.
   */
  private ConnectionInfo connectionInfo;

  /**
   * The name of the relation (RelationKey) for a SELECT * query.
   */
  private RelationKey relationKey;

  /**
   * Iterate over data from the JDBC database.
   * */
  private transient Iterator<TupleBatch> tuples;
  /**
   * The result schema.
   * */
  private final Schema outputSchema;

  /**
   * The SQL template.
   * */
  private String baseSQL;

  /**
   * Column indexes that the output should be ordered by.
   */
  private final int[] sortedColumns;

  /**
   * True for each column in {@link #sortedColumns} that should be ordered ascending.
   */
  private final boolean[] ascending;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(DbQueryScan.class);

  /**
   * Constructor.
   *
   * @param baseSQL see the corresponding field.
   * @param outputSchema see the corresponding field.
   * */
  public DbQueryScan(final String baseSQL, final Schema outputSchema) {
    this.baseSQL = Objects.requireNonNull(baseSQL);
    this.outputSchema = Objects.requireNonNull(outputSchema);
    sortedColumns = null;
    ascending = null;
  }

  /**
   * Constructor that receives the connection info as input.
   *
   * @param connectionInfo see the corresponding field.
   * @param baseSQL see the corresponding field.
   * @param outputSchema see the corresponding field.
   * */
  public DbQueryScan(
      final ConnectionInfo connectionInfo, final String baseSQL, final Schema outputSchema) {
    this(baseSQL, outputSchema);
    this.connectionInfo = Objects.requireNonNull(connectionInfo);
  }

  /**
   * Construct a new DbQueryScan object that simply runs <code>SELECT * FROM relationKey</code>.
   *
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   */
  public DbQueryScan(final RelationKey relationKey, final Schema outputSchema) {
    this.relationKey = Objects.requireNonNull(relationKey);
    this.outputSchema = Objects.requireNonNull(outputSchema);
    sortedColumns = null;
    ascending = null;
  }

  /**
   * Construct a new DbQueryScan object that simply runs <code>SELECT * FROM relationKey</code>, but receiving the
   * connection info as input.
   *
   * @param connectionInfo the connection information.
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   */
  public DbQueryScan(
      final ConnectionInfo connectionInfo,
      final RelationKey relationKey,
      final Schema outputSchema) {
    this(relationKey, outputSchema);
    this.connectionInfo = Objects.requireNonNull(connectionInfo);
  }

  /**
   * Construct a new DbQueryScan object that runs <code>SELECT * FROM relationKey ORDER BY [...]</code>.
   *
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   * @param sortedColumns the columns by which the tuples should be ordered by.
   * @param ascending true for columns that should be ordered ascending.
   */
  public DbQueryScan(
      final RelationKey relationKey,
      final Schema outputSchema,
      final int[] sortedColumns,
      final boolean[] ascending) {
    this.relationKey = Objects.requireNonNull(relationKey);
    this.outputSchema = Objects.requireNonNull(outputSchema);
    this.sortedColumns = sortedColumns;
    this.ascending = ascending;
  }

  /**
   * Construct a new DbQueryScan object that runs <code>SELECT * FROM relationKey ORDER BY [...]</code>, but receiving
   * the connection info as input.
   *
   * @param connectionInfo the connection information.
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   * @param sortedColumns the columns by which the tuples should be ordered by.
   * @param ascending true for columns that should be ordered ascending.
   */
  public DbQueryScan(
      final ConnectionInfo connectionInfo,
      final RelationKey relationKey,
      final Schema outputSchema,
      final int[] sortedColumns,
      final boolean[] ascending) {
    this(relationKey, outputSchema, sortedColumns, ascending);
    Objects.requireNonNull(connectionInfo);
    this.connectionInfo = connectionInfo;
  }

  @Override
  public final void cleanup() {
    tuples = null;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    Objects.requireNonNull(connectionInfo);
    if (tuples == null) {
      tuples =
          AccessMethod.of(connectionInfo.getDbms(), connectionInfo, true)
              .tupleBatchIteratorFromQuery(baseSQL, outputSchema);
    }
    if (tuples.hasNext()) {
      final TupleBatch tb = tuples.next();
      LOGGER.trace("Got {} tuples", tb.numTuples());
      return tb;
    } else {
      return null;
    }
  }

  @Override
  public final Schema generateSchema() {
    return outputSchema;
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    if (connectionInfo == null) {
      final String dbms = (String) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM);
      if (dbms == null) {
        throw new DbException("Unable to instantiate DbQueryScan: database system unknown");
      }

      connectionInfo =
          (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
      if (connectionInfo == null) {
        throw new DbException("Unable to instantiate DbQueryScan: connection information unknown");
      }

      if (!dbms.equals(connectionInfo.getDbms())) {
        throw new DbException(
            "Unable to instantiate DbQueryScan: database system does not conform with connection information");
      }
    }

    if (relationKey != null) {
      baseSQL = "SELECT * FROM " + relationKey.toString(connectionInfo.getDbms());

      String prefix = "";
      if (sortedColumns != null && sortedColumns.length > 0) {
        Preconditions.checkArgument(sortedColumns.length == ascending.length);
        StringBuilder orderByClause = new StringBuilder(" ORDER BY");

        for (int columnIdx : sortedColumns) {
          orderByClause.append(prefix + " " + getSchema().getColumnName(columnIdx));
          if (ascending[columnIdx]) {
            orderByClause.append(" ASC");
          } else {
            orderByClause.append(" DESC");
          }

          prefix = ",";
        }

        baseSQL = baseSQL.concat(orderByClause.toString());
      }
    }
  }

  /**
   * @return the connection info in this DbQueryScan.
   */
  public ConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }

  @Override
  public Set<RelationKey> readSet() {
    if (relationKey == null) {
      LOGGER.error("DbQueryScan does not support the DbReader interface properly for SQL queries.");
      return ImmutableSet.of();
    }
    return ImmutableSet.of(relationKey);
  }
}
