/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;

/**
 *
 */
public class PersistedDatasetEncoding extends MyriaApiEncoding {
  @Required public RelationKey relationKey;
  @Required public Schema schema;
  @Required public String rootUri;
  @Required public Integer numWorkers;
  @Required public DistributeFunction distributeFunction;

  @JsonCreator
  public PersistedDatasetEncoding(
      RelationKey relationKey,
      Schema schema,
      String rootUri,
      Integer numWorkers,
      DistributeFunction distributeFunction) {
    this.relationKey = relationKey;
    this.schema = schema;
    this.relationKey = relationKey;
    this.rootUri = rootUri;
    this.numWorkers = numWorkers;
    this.distributeFunction = distributeFunction;
  }
}
