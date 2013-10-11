package edu.washington.escience.myria.api.encoding;

import java.net.URI;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;

/**
 * Metadata about a dataset that has been loaded into the system.
 * 
 * @author dhalperi
 * 
 */
public class DatasetStatus {
  /**
   * Instantiate a DatasetStatus with the provided values.
   * 
   * @param relationKey The {@link RelationKey} identifying the dataset.
   * @param schema The {@link Schema} of the tuples in the dataset.
   * @param numTuples The number of tuples in the dataset.
   * @param queryId The query that created this dataset.
   * @param created When this dataset was created, in ISO8601 format.
   */
  public DatasetStatus(final RelationKey relationKey, final Schema schema, final long numTuples, final long queryId, final String created) {
    this.relationKey = relationKey;
    this.schema = schema;
    this.numTuples = numTuples;
    this.queryId = queryId;
    this.created = created;
  }

  /** The {@link RelationKey} identifying the dataset. */
  private final RelationKey relationKey;
  /** The {@link Schema} of the tuples in the dataset. */
  private final Schema schema;
  /** The number of tuples in the dataset. */
  private final Long numTuples;
  /** The query that created this dataset. */
  private final Long queryId;
  /** When this dataset was created, in ISO8601 format. */
  private final String created;
  /** The URI of this resource. */
  public URI uri;

  /**
   * @return The {@link RelationKey} identifying the dataset.
   */
  public RelationKey getRelationKey() {
    return relationKey;
  }

  /**
   * @return The {@link Schema} of the tuples in the dataset.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * @return The number of tuples in the dataset.
   */
  public long getNumTuples() {
    return numTuples;
  }

  /**
   * @return the queryId
   */
  public Long getQueryId() {
    return queryId;
  }

  /**
   * @return the created
   */
  public String getCreated() {
    return created;
  }

  /**
   * Set the URI of this dataset.
   * 
   * @param datasetUri
   */
  public void setUri(final URI datasetUri) {
    uri = datasetUri;
  }
}
