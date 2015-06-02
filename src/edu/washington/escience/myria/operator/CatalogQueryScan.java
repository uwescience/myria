package edu.washington.escience.myria.operator;

import java.util.Iterator;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.coordinator.catalog.MasterCatalog;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Push a select query down into a JDBC based database and scan over the query result.
 * */
public class CatalogQueryScan extends LeafOperator {

  /**
   * Iterate over data from the catalog.
   */
  private transient Iterator<TupleBatch> tuples;

  /**
   * The result schema.
   * */
  private final Schema outputSchema;

  /**
   * The SQL query.
   */
  private final String sql;

  /**
   * The master catalog.
   */
  private final MasterCatalog catalog;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(CatalogQueryScan.class);

  /**
   * Constructor.
   * 
   * @param sql see the corresponding field.
   * @param outputSchema see the corresponding field.
   * @param catalog see the corresponding field.
   * */
  public CatalogQueryScan(final String sql, final Schema outputSchema, final MasterCatalog catalog) {
    Objects.requireNonNull(sql);
    Objects.requireNonNull(outputSchema);
    Objects.requireNonNull(catalog);

    this.sql = sql;
    this.outputSchema = outputSchema;
    this.catalog = catalog;
    tuples = null;
  }

  @Override
  public final void cleanup() {
    tuples = null;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    if (tuples == null) {
      try {
        tuples = catalog.tupleBatchIteratorFromQuery(sql, outputSchema);
      } catch (CatalogException e) {
        throw new DbException(e);
      }
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
  }
}
