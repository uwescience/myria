package edu.washington.escience.myriad.operator;

import java.util.Iterator;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.AccessMethod;
import edu.washington.escience.myriad.accessmethod.ConnectionInfo;
import edu.washington.escience.myriad.parallel.CollectConsumer;

/**
 * Push a select query down into a JDBC based database and scan over the query result.
 * */
public class QueryScan extends LeafOperator {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectConsumer.class);

  /**
   * The connection info.
   */
  private ConnectionInfo connectionInfo;

  /**
   * The dbms.
   */
  private String dbms;

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

  /**
   * Constructor.
   * 
   * @param baseSQL see the corresponding field.
   * @param outputSchema see the corresponding field.
   * */
  public QueryScan(final String baseSQL, final Schema outputSchema) {
    Objects.requireNonNull(baseSQL);
    Objects.requireNonNull(outputSchema);

    this.baseSQL = baseSQL;
    this.outputSchema = outputSchema;
    tuples = null;
  }

  /**
   * Construct a new QueryScan object that simply runs <code>SELECT * FROM relationKey</code>.
   * 
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   */
  public QueryScan(final RelationKey relationKey, final Schema outputSchema) {
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(outputSchema);

    this.relationKey = relationKey;
    this.outputSchema = outputSchema;
    baseSQL = null;
    tuples = null;
  }

  @Override
  public final void cleanup() {
    tuples = null;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    if (tuples == null) {
      tuples = AccessMethod.of(dbms, connectionInfo, true).tupleBatchIteratorFromQuery(baseSQL, outputSchema);
    }
    if (tuples.hasNext()) {
      final TupleBatch tb = tuples.next();
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
    dbms = (String) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_SYSTEM);
    if (dbms == null) {
      throw new DbException("Unable to instantiate QueryScan: database system unknown");
    }

    connectionInfo = (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
    if (connectionInfo == null) {
      throw new DbException("Unable to instantiate QueryScan: connection information unknown");
    }

    if (relationKey != null) {
      baseSQL = "SELECT * FROM " + relationKey.toString(dbms);
    }
  }
}
