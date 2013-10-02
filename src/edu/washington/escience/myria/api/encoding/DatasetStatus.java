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
   */
  public DatasetStatus(final RelationKey relationKey, final Schema schema, final long numTuples) {
    this.relationKey = relationKey;
    this.schema = schema;
    this.numTuples = numTuples;
  }

  /** The {@link RelationKey} identifying the dataset. */
  private final RelationKey relationKey;
  /** The {@link Schema} of the tuples in the dataset. */
  private final Schema schema;
  /** The number of tuples in the dataset. */
  private final Long numTuples;
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
   * Set the URI of this dataset.
   * 
   * @param datasetUri
   */
  public void setUri(final URI datasetUri) {
    uri = datasetUri;
  }
}
