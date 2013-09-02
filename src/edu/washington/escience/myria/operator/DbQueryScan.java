package edu.washington.escience.myria.operator;

import java.util.Iterator;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;

/**
 * Push a select query down into a JDBC based database and scan over the query result.
 * */
public class DbQueryScan extends LeafOperator {

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

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(DbQueryScan.class);

  /**
   * Constructor.
   * 
   * @param baseSQL see the corresponding field.
   * @param outputSchema see the corresponding field.
   * */
  public DbQueryScan(final String baseSQL, final Schema outputSchema) {
    Objects.requireNonNull(baseSQL);
    Objects.requireNonNull(outputSchema);

    this.baseSQL = baseSQL;
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
    Objects.requireNonNull(connectionInfo);
    this.connectionInfo = connectionInfo;
  }

  /**
   * Construct a new DbQueryScan object that simply runs <code>SELECT * FROM relationKey</code>.
   * 
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   */
  public DbQueryScan(final RelationKey relationKey, final Schema outputSchema) {
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(outputSchema);

    this.relationKey = relationKey;
    this.outputSchema = outputSchema;
    baseSQL = null;
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
          AccessMethod.of(connectionInfo.getDbms(), connectionInfo, true).tupleBatchIteratorFromQuery(baseSQL,
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
  public final Schema getSchema() {
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

    if (relationKey != null) {
      baseSQL = "SELECT * FROM " + relationKey.toString(connectionInfo.getDbms());
    }
  }
}
