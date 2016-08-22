/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.AmazonS3Source;

/**
 * Table Description Encoding
 */
public class PerfEnforceTableEncoding {
  public RelationKey relationKey;
  public String type;
  public AmazonS3Source source;
  public Schema schema;
  public Character delimiter;
  public Set<Integer> keys;

  public Set<Integer> corresponding_fact_key;

  /**
   * @param relationKey
   * @param type
   * @param source
   * @param schema
   * @param delimiter
   * @param keys
   * @param corresponding_fact_key
   */
  @JsonCreator
  public PerfEnforceTableEncoding(
      @JsonProperty(value = "relationKey", required = true) final RelationKey relationKey,
      @JsonProperty(value = "type", required = true) final String type,
      @JsonProperty(value = "source", required = true) final AmazonS3Source source,
      @JsonProperty(value = "schema", required = true) final Schema schema,
      @JsonProperty(value = "delimiter", required = true) final Character delimiter,
      @JsonProperty(value = "keys", required = true) final Set<Integer> keys,
      @JsonProperty("corresponding_fact_key") final Set<Integer> corresponding_fact_key) {

    this.relationKey = relationKey;
    this.type = type;
    this.source = source;
    this.schema = schema;
    this.delimiter = delimiter;
    this.keys = keys;
    this.corresponding_fact_key = corresponding_fact_key;
  }
}
