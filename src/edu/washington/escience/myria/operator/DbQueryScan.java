package edu.washington.escience.myria.operator;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.expression.evaluate.SqlExpressionOperatorParameter;
import edu.washington.escience.myria.expression.sql.ColumnReferenceExpression;
import edu.washington.escience.myria.expression.sql.SqlQuery;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Push a select query down into a JDBC based database and scan over the query result.
 */
public class DbQueryScan extends LeafOperator {
  /**
   * The connection info.
   */
  private ConnectionInfo connectionInfo;

  /**
   * The result schema.
   */
  private final Schema outputSchema;

  /**
   * The SQL template.
   */
  private String rawSqlQuery;

  /**
   * SQL Ast.
   */
  private final SqlQuery query;

  /**
   * Iterate over data from the JDBC database.
   * */
  private transient Iterator<TupleBatch> tuples;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DbQueryScan.class);

  /**
   * @param rawSqlQuery see the corresponding field.
   * @param outputSchema see the corresponding field.
   */
  public DbQueryScan(final String rawSqlQuery, final Schema outputSchema) {
    Objects.requireNonNull(rawSqlQuery, "rawSqlQuery");
    Objects.requireNonNull(outputSchema, "outputSchema");

    this.rawSqlQuery = rawSqlQuery;
    this.outputSchema = outputSchema;
    query = null;
    connectionInfo = null;
    tuples = null;
  }

  /**
   * @param query the AST.
   * @param outputSchema see the corresponding field.
   */
  public DbQueryScan(final SqlQuery query, final Schema outputSchema) {
    Objects.requireNonNull(query, "query");
    Objects.requireNonNull(outputSchema, "outputSchema");

    this.query = query;
    this.outputSchema = outputSchema;
    connectionInfo = null;
    tuples = null;
  }

  /**
   * Constructor that receives the connection info as input.
   * 
   * @param connectionInfo see the corresponding field.
   * @param baseSQL see the corresponding field.
   * @param outputSchema see the corresponding field.
   * */
  public DbQueryScan(final ConnectionInfo connectionInfo, final String baseSQL, final Schema outputSchema) {
    this(baseSQL, outputSchema);
    Objects.requireNonNull(connectionInfo, "connectionInfo");
    this.connectionInfo = connectionInfo;
  }

  /**
   * Construct a new DbQueryScan object that simply runs <code>SELECT * FROM relationKey</code>.
   * 
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   */
  public DbQueryScan(final RelationKey relationKey, final Schema outputSchema) {
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(outputSchema, "outputSchema");

    query = new SqlQuery(relationKey);

    this.outputSchema = outputSchema;
    rawSqlQuery = null;
    connectionInfo = null;
    tuples = null;
  }

  /**
   * Construct a new DbQueryScan object that simply runs <code>SELECT * FROM relationKey</code>, but receiving the
   * connection info as input.
   * 
   * @param connectionInfo the connection information.
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   */
  public DbQueryScan(final ConnectionInfo connectionInfo, final RelationKey relationKey, final Schema outputSchema) {
    this(relationKey, outputSchema);
    Objects.requireNonNull(connectionInfo, "connectionInfo");
    this.connectionInfo = connectionInfo;
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
  public DbQueryScan(final ConnectionInfo connectionInfo, final RelationKey relationKey, final Schema outputSchema,
      final List<ColumnReferenceExpression> sortedColumns, final List<Boolean> ascending) {
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(outputSchema, "outputSchema");
    Objects.requireNonNull(connectionInfo, "connectionInfo");

    LinkedHashMap<RelationKey, Schema> schemas = Maps.newLinkedHashMap();
    schemas.put(relationKey, outputSchema);
    query = new SqlQuery(null, schemas, null, sortedColumns, ascending);
    this.outputSchema = outputSchema;
    this.connectionInfo = connectionInfo;
    rawSqlQuery = null;
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
          AccessMethod.of(connectionInfo.getDbms(), connectionInfo, true).tupleBatchIteratorFromQuery(rawSqlQuery,
              outputSchema);
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

      connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
      if (connectionInfo == null) {
        throw new DbException("Unable to instantiate DbQueryScan: connection information unknown");
      }

      if (!dbms.equals(connectionInfo.getDbms())) {
        throw new DbException(
            "Unable to instantiate DbQueryScan: database system does not conform with connection information");
      }
    }

    if (rawSqlQuery == null) {
      Objects.requireNonNull(query, "query");
      final SqlExpressionOperatorParameter parameters =
          new SqlExpressionOperatorParameter(connectionInfo.getDbms(), getNodeID());
      rawSqlQuery = query.getSqlString(parameters);
    }
  }

  /**
   * @return the connection info in this DbQueryScan.
   */
  public ConnectionInfo getConnectionInfo() {
    return connectionInfo;
  }
}
