package edu.washington.escience.myria.parallel;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.coordinator.catalog.CatalogException;
import edu.washington.escience.myria.coordinator.catalog.MasterCatalog;
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
  public DatasetMetadataUpdater(final MasterCatalog catalog, final Map<Integer, SubQueryPlan> workerPlans,
      final long queryId) throws CatalogException {
    this.catalog = Objects.requireNonNull(catalog);
    this.queryId = queryId;
    relationsCreated = inferRelationsCreated(Objects.requireNonNull(workerPlans), catalog);
    if (!relationsCreated.isEmpty()) {
      LOGGER.debug("DatasetMetadataUpdater configured for query #{} with relations-worker map {}", queryId,
          relationsCreated);
    }
  }

  @Override
  public void operationComplete(final OperationFuture future) throws Exception {
    if (relationsCreated.isEmpty()) {
      /* Do nothing */
      return;
    }

    if (!future.isSuccess()) {
      LOGGER.debug("Query #{} failed, so not updating the catalog metadata for relations {}.", queryId,
          relationsCreated.keySet());
      return;
    }

    LOGGER.debug("Query #{} succeeded, so updating the catalog metadata for relations {}.", queryId, relationsCreated
        .keySet());
    for (Map.Entry<RelationKey, RelationMetadata> entry : relationsCreated.entrySet()) {
      RelationKey relation = entry.getKey();
      RelationMetadata meta = entry.getValue();
      Set<Integer> workers = meta.getWorkers();
      Schema schema = meta.getSchema();
      if (catalog.getSchema(relation) == null) {
        catalog.addRelationMetadata(relation, schema, -1, queryId);
      }
      catalog.addStoredRelation(relation, workers, "unknown");
      LOGGER.debug("Query #{} - adding {} to store shard of {}", queryId, workers, relation
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
  private static Map<RelationKey, RelationMetadata> inferRelationsCreated(final Map<Integer, SubQueryPlan> workerPlans,
      final MasterCatalog catalog) throws CatalogException {
    Map<RelationKey, RelationMetadata> ret = new HashMap<RelationKey, RelationMetadata>();

    /* Loop through each subquery plan, finding what relations it writes. */
    for (Map.Entry<Integer, SubQueryPlan> entry : workerPlans.entrySet()) {
      Integer workerId = entry.getKey();
      SubQueryPlan plan = entry.getValue();
      Map<RelationKey, Schema> writes = plan.writeSet();

      /* For every relation this plan writes, create a new relation metadata. */
      for (RelationKey relation : writes.keySet()) {
        if (!ret.containsKey(relation)) {
          RelationMetadata meta = new RelationMetadata(relation);
          ret.put(relation, meta);
        }
      }

      /* Ensure that the metadata are consistent with both the existing Catalog and across the subquery plan fragments. */
      for (Map.Entry<RelationKey, Schema> writeEntry : writes.entrySet()) {
        RelationKey relationKey = writeEntry.getKey();
        Schema newSchema = writeEntry.getValue();

        /* Check if the relation already has a schema in the database and, if so, it better match! */
        Schema oldSchema = catalog.getSchema(relationKey);
        if (oldSchema != null) {
          Preconditions.checkArgument(oldSchema.equals(newSchema),
              "Relation %s already exists with Schema %s. You cannot overwrite it with the new Schema %s", relationKey,
              oldSchema, newSchema);
        }

        /* Ensure that the metadata for this {@link SubQuery} also agrees about the schema, add this worker. */
        RelationMetadata meta = ret.get(relationKey);
        meta.setOrVerifySchema(newSchema);
        meta.addWorker(workerId);
      }
    }
    return ret;
  }

  /** Class to store the metadata for a relation between query issuing and query complete. */
  private static class RelationMetadata {
    /** The relation. */
    private final RelationKey relationKey;
    /** The workers that will store the relation. */
    private Set<Integer> workers;
    /** The schema of the relation. */
    private Schema schema;

    /**
     * Construct a new {@link RelationMetadata} instance.
     * 
     * @param relationKey the relation.
     */
    RelationMetadata(final RelationKey relationKey) {
      this.relationKey = Objects.requireNonNull(relationKey, "relationKey");
      workers = ImmutableSet.of();
    }

    /**
     * @return the workers.
     */
    public ImmutableSet<Integer> getWorkers() {
      return ImmutableSet.copyOf(workers);
    }

    /**
     * @param worker the worker to add
     */
    public void addWorker(final int worker) {
      workers = ImmutableSet.<Integer> builder().addAll(workers).add(worker).build();
    }

    /**
     * @return the schema.
     */
    public Schema getSchema() {
      return schema;
    }

    /**
     * If there is not yet a Schema defined for this relation, set it. Otherwise, make sure that the provided Schema
     * matches the existing definition.
     * 
     * @param schema the schema to set.
     */
    public void setOrVerifySchema(final Schema schema) {
      if (this.schema == null) {
        this.schema = Objects.requireNonNull(schema, "schema");
        return;
      }
      Preconditions.checkArgument(this.schema.equals(schema),
          "SubQuery cannot write Relation %s with two different Schemas %s and %s", relationKey, this.schema, schema);
    }
  }
}
