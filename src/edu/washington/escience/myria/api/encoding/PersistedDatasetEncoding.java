/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;

/**
 *
 */
public class PersistedDatasetEncoding extends MyriaApiEncoding {
  /** The {@link RelationKey} identifying the dataset. */
  @JsonProperty private final RelationKey relationKey;
  /** The {@link Schema} of the tuples in the dataset. */
  @JsonProperty private final Schema schema;
  /** The root URI of this relation's stored partitions. */
  @JsonProperty private String rootUri;
  /** The number of workers storing the dataset. */
  @JsonProperty private final Integer numWorkers;
  /** The tuple distribution function. */
  @JsonProperty private final DistributeFunction distributeFunction;

  @JsonCreator
  public PersistedDatasetEncoding(
      RelationKey relationKey,
      Schema schema,
      String rootUri,
      Integer numWorkers,
      DistributeFunction distributeFunction) {
    this.relationKey = relationKey;
    this.schema = schema;
    this.rootUri = rootUri;
    this.numWorkers = numWorkers;
    this.distributeFunction = distributeFunction;
  }

  /** @return The {@link RelationKey} identifying the dataset. */
  public RelationKey getRelationKey() {
    return relationKey;
  }

  /** @return The {@link Schema} of the tuples in the dataset. */
  public Schema getSchema() {
    return schema;
  }

  /** @return The root URI of this relation's stored partitions. */
  public String getRootUri() {
    return rootUri;
  }

  /** @return The number of workers storing the dataset. */
  public int getNumWorkers() {
    return numWorkers;
  }

  /** @return The tuple distribution function. */
  public DistributeFunction getDistributeFunction() {
    return distributeFunction;
  }
}
