package edu.washington.escience.myria.api.encoding;

import java.net.URI;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.network.partition.HowPartitioned;

/**
 * Metadata about a dataset that has been loaded into the system.
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
   * @param howPartitioned How this dataset was partitioned.
   */
  @JsonCreator
  public DatasetStatus(
      @JsonProperty("relationKey") final RelationKey relationKey,
      @JsonProperty("schema") final Schema schema,
      @JsonProperty("numTuples") final long numTuples,
      @JsonProperty("queryId") final long queryId,
      @JsonProperty("created") final String created,
      @JsonProperty("howPartitioned") final HowPartitioned howPartitioned) {
    this.relationKey = relationKey;
    this.schema = schema;
    this.numTuples = numTuples;
    this.queryId = queryId;
    this.created = created;
    this.howPartitioned = howPartitioned;
  }

  /** The {@link RelationKey} identifying the dataset. */
  @JsonProperty private final RelationKey relationKey;
  /** The {@link Schema} of the tuples in the dataset. */
  @JsonProperty private final Schema schema;
  /** The number of tuples in the dataset. */
  @JsonProperty private final Long numTuples;
  /** The query that created this dataset. */
  @JsonProperty private final Long queryId;
  /** When this dataset was created, in ISO8601 format. */
  @JsonProperty private final String created;
  /** How this dataset was partitioned. */
  @JsonProperty private final HowPartitioned howPartitioned;
  /** The URI of this resource. */
  @JsonProperty public URI uri;

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
   * @return how the dataset was partitioned.
   */
  public HowPartitioned getHowPartitioned() {
    return howPartitioned;
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
