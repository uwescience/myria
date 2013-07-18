package edu.washington.escience.myriad.operator;

import java.util.Iterator;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;

/**
 * Push a select query down into SQLite and scan over the query result.
 * */
public class SQLiteQueryScan extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * Iterate over data from the SQLite database.
   * */
  private transient Iterator<TupleBatch> tuples;
  /**
   * The result schema.
   * */
  private final Schema schema;
  /**
   * the SQL template.
   * */
  private final String baseSQL;

  /**
   * The SQLite DB filepath.
   * */
  private transient String databaseFilename;

  /**
   * Construct a new SQLiteQueryScan object that simply runs <code>SELECT * FROM relationKey</code>.
   * 
   * @param relationKey the relation to be scanned.
   * @param outputSchema the Schema of the returned tuples.
   */
  public SQLiteQueryScan(final RelationKey relationKey, final Schema outputSchema) {
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(outputSchema);
    baseSQL = "SELECT * FROM " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_SQLITE);
    schema = outputSchema;
  }

  /**
   * Construct a new SQLiteQueryScan object.
   * 
   * @param baseSQL the selection query.
   * @param outputSchema the Schema of the returned tuples.
   */
  public SQLiteQueryScan(final String baseSQL, final Schema outputSchema) {
    Objects.requireNonNull(baseSQL);
    Objects.requireNonNull(outputSchema);
    this.baseSQL = baseSQL;
    schema = outputSchema;
  }

  @Override
  public final void cleanup() {
    tuples = null;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    if (tuples == null) {
      tuples = SQLiteAccessMethod.tupleBatchIteratorFromQuery(databaseFilename, baseSQL, schema);
    }
    if (tuples.hasNext()) {
      return tuples.next();
    } else {
      return null;
    }
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  @Override
  public final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final String sqliteDatabaseFilename = (String) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_NAME);
    if (sqliteDatabaseFilename == null) {
      throw new DbException("Unable to instantiate SQLiteQueryScan on non-sqlite worker");
    }
    databaseFilename = sqliteDatabaseFilename;
    tuples = null;
  }

  @Override
  public final String toString() {
    return "SQLiteQueryScan, baseSQL: " + baseSQL;
  }
}
