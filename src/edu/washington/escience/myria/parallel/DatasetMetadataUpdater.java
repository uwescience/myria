package edu.washington.escience.myria.parallel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.coordinator.catalog.MasterCatalog;
import edu.washington.escience.myria.operator.DbInsert;
import edu.washington.escience.myria.operator.RootOperator;
import edu.washington.escience.myria.util.concurrent.OperationFuture;
import edu.washington.escience.myria.util.concurrent.OperationFutureListener;

/**
 * This class updates the Catalog metadata to reflect datasets that have been created by a query. This metadata updating
 * is triggered when the query finishes.
 * 
 * @author dhalperi
 * 
 */
public final class DatasetMetadataUpdater implements OperationFutureListener {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetMetadataUpdater.class);

  /** The catalog which will be updated with the new relation metadata. */
  private final MasterCatalog catalog;
  /** The metadata for each relation. */
  private final Map<RelationKey, RelationMetadata> relationsCreated;
  /** The query id. */
  private final long queryId;

  /**
   * Create a new DatasetMetadataUpdater, which will update the specified catalog to reflect the creation of the
   * specified datasets, each stored on its specified set of workers, when the query it is listening to finishes
   * successfully.
   * 
   * @param catalog the MasterCatalog that will be updated.
   * @param workerPlans the plans of the queries being executed at the workers.
   * @param queryId the query that will write these relations to the cluster.
   * @throws CatalogException if there is an error accessing the catalog.
   */
  public DatasetMetadataUpdater(final MasterCatalog catalog, final Map<Integer, SingleQueryPlanWithArgs> workerPlans,
      final long queryId) throws CatalogException {
    this.catalog = Objects.requireNonNull(catalog);
    this.queryId = queryId;
    relationsCreated = inferRelationsCreated(Objects.requireNonNull(workerPlans), catalog);
    LOGGER.info("DatasetMetadatUpdater configured for query #{} with relations-worker map {}", queryId,
        relationsCreated);
  }

  @Override
  public void operationComplete(final OperationFuture future) throws Exception {
    if (!future.isSuccess()) {
      LOGGER.info("Query #{} failed, so not updating the catalog metadata for relations {}.", queryId, relationsCreated
          .keySet());
      return;
    }

    LOGGER.info("Query #{} succeeded, so updating the catalog metadata for relations.", queryId);
    for (Map.Entry<RelationKey, RelationMetadata> entry : relationsCreated.entrySet()) {
      RelationKey relation = entry.getKey();
      RelationMetadata meta = entry.getValue();
      Set<Integer> workers = meta.getWorkers();
      Schema schema = meta.getSchema();
      if (catalog.getSchema(relation) == null) {
        catalog.addRelationMetadata(relation, schema, -1, queryId);
      }
      catalog.addStoredRelation(relation, workers, "unknown");
      LOGGER.info("Query #{} - adding {} to store shard of {}", queryId, workers, relation
          .toString(MyriaConstants.STORAGE_SYSTEM_SQLITE));
    }
  }

  /**
   * Helper function -- given a set of worker plans, identify what relations were created and on which workers. Also add
   * any new relations discovered to the MasterCatalog.
   * 
   * @param workerPlans the worker plans.
   * @param catalog the MasterCatalog for the cluster that executed this query.
   * @return a mapping showing what relations were created and on which workers.
   * @throws CatalogException if there is an error in the Catalog.
   */
  private static Map<RelationKey, RelationMetadata> inferRelationsCreated(
      final Map<Integer, SingleQueryPlanWithArgs> workerPlans, final MasterCatalog catalog) throws CatalogException {
    Map<RelationKey, RelationMetadata> ret = new HashMap<RelationKey, RelationMetadata>();

    /* For each plan, look for DbInsert operators. */
    for (Map.Entry<Integer, SingleQueryPlanWithArgs> entry : workerPlans.entrySet()) {
      SingleQueryPlanWithArgs plan = entry.getValue();
      Integer workerId = entry.getKey();
      List<RootOperator> rootOps = plan.getRootOps();
      for (RootOperator op : rootOps) {
        /* If op is a DbInsert, we are inserting tuples into a relation. */
        if (op instanceof DbInsert) {
          /* Add this worker to the set of workers storing the new copy of this relation. */
          RelationKey relationKey = ((DbInsert) op).getRelationKey();
          RelationMetadata meta = ret.get(relationKey);
          if (meta == null) {
            meta = new RelationMetadata();
            meta.setWorkers(new TreeSet<Integer>());
            ret.put(relationKey, meta);
          }
          meta.getWorkers().add(workerId);
          Schema newSchema = op.getSchema();
          Schema oldSchema = catalog.getSchema(relationKey);
          /* Check if the relation already has a schema in the database and, if so, it better match! */
          if (oldSchema != null) {
            Preconditions.checkArgument(oldSchema.equals(newSchema),
                "Relation %s already exists with Schema %s. You cannot overwrite it with the new Schema %s", relationKey,
                oldSchema, newSchema);
          }
          meta.setSchema(newSchema);
        }
      }
    }
    return ret;
  }

  /** Class to store the metadata for a relation between query issuing and query complete. */
  private static class RelationMetadata {
    /** The workers that will store the relation. */
    private Set<Integer> workers;
    /** The schema of the relation. */
    private Schema schema;

    /**
     * @return the workers.
     */
    public Set<Integer> getWorkers() {
      return workers;
    }

    /**
     * @param workers the workers to set,
     */
    public void setWorkers(final Set<Integer> workers) {
      this.workers = Objects.requireNonNull(workers);
    }

    /**
     * @return the schema.
     */
    public Schema getSchema() {
      return schema;
    }

    /**
     * @param schema the schema to set.
     */
    public void setSchema(final Schema schema) {
      this.schema = Objects.requireNonNull(schema);
    }
  }
}
