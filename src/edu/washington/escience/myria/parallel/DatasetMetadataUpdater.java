package edu.washington.escience.myria.parallel;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.coordinator.catalog.MasterCatalog;
import edu.washington.escience.myria.util.concurrent.OperationFuture;
import edu.washington.escience.myria.util.concurrent.OperationFutureListener;

/**
 * This class updates the Catalog metadata to reflect datasets that have been created by a query. This metadata updating
 * is triggered when the query finishes.
 * 
 * 
 */
public final class DatasetMetadataUpdater implements OperationFutureListener {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetMetadataUpdater.class);

  /** The catalog which will be updated with the new relation metadata. */
  private final MasterCatalog catalog;
  /** The metadata for each relation. */
  private final Map<RelationKey, RelationWriteMetadata> relationsCreated;
  /** The query id. */
  private final SubQueryId subQueryId;

  /**
   * Create a new DatasetMetadataUpdater, which will update the specified catalog to reflect the creation of the
   * specified datasets, each stored on its specified set of workers, when the query it is listening to finishes
   * successfully.
   * 
   * @param catalog the MasterCatalog that will be updated.
   * @param metadata informationa about the relations created by subquery.
   * @param subQueryId the subquery that will write these relations to the cluster.
   */
  public DatasetMetadataUpdater(@Nonnull final MasterCatalog catalog,
      final Map<RelationKey, RelationWriteMetadata> metadata, @Nonnull final SubQueryId subQueryId) {
    this.catalog = Objects.requireNonNull(catalog, "catalog");
    this.subQueryId = Objects.requireNonNull(subQueryId, "subQueryId");
    relationsCreated = Objects.requireNonNull(metadata, "metadata");
    Preconditions.checkArgument(!relationsCreated.isEmpty(),
        "DatasetMetadataUpdater should not be created for subqueries that create no relations");
    LOGGER.debug("DatasetMetadataUpdater configured for query #{} with relations-worker map {}", subQueryId,
        relationsCreated);
  }

  @Override
  public void operationComplete(final OperationFuture future) throws Exception {
    if (!future.isSuccess()) {
      LOGGER.debug("SubQuery #{} failed, so not updating the catalog metadata for relations {}.", subQueryId,
          relationsCreated.keySet());
      return;
    }

    LOGGER.debug("SubQuery #{} succeeded, so updating the catalog metadata for relations {}.", subQueryId,
        relationsCreated.keySet());
    for (RelationWriteMetadata meta : relationsCreated.values()) {
      RelationKey relation = meta.getRelationKey();
      Set<Integer> workers = meta.getWorkers();
      if (meta.isOverwrite()) {
        catalog.deleteRelationIfExists(meta.getRelationKey());
      }
      Schema schema = meta.getSchema();
      if (catalog.getSchema(relation) == null) {
        /* Overwrite or new relation. */
        catalog.addRelationMetadata(relation, schema, -1, subQueryId.getQueryId());
        catalog.addStoredRelation(relation, workers, "unknown");
        LOGGER.debug("SubQuery #{} - adding {} to store shard of {}", subQueryId.getQueryId(), workers, relation
            .toString());
      }
    }
  }
}
