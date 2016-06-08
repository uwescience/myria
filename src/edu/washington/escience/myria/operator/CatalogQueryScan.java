package edu.washington.escience.myria.operator;

import java.util.Iterator;
import java.util.Objects;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.coordinator.CatalogException;
import edu.washington.escience.myria.coordinator.MasterCatalog;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Operator to get the result of a query on the catalog. The catalog is a SQLite database.
 */
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
  private final transient MasterCatalog catalog;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The logger for debug, trace, etc. messages in this class. */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(CatalogQueryScan.class);

  /**
   * Constructor.
   *
   * @param sql see the corresponding field.
   * @param outputSchema see the corresponding field.
   * @param catalog see the corresponding field.
   * */
  public CatalogQueryScan(
      final String sql, final Schema outputSchema, final MasterCatalog catalog) {
    this.sql = Objects.requireNonNull(sql);
    ;
    this.outputSchema = Objects.requireNonNull(outputSchema);
    this.catalog = Objects.requireNonNull(catalog);
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
}
