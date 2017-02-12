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
 * This class returns an encoding describing a table's schema.
 */
public class PerfEnforceTableEncoding {
  /** The relationkey of the relation */
  public RelationKey relationKey;
  /** The type of the relation, which can be either "Dimension" or "fact". */
  public String type;
  /** The S3 source of the relation. */
  public AmazonS3Source source;
  /** The schema of the relation. */
  public Schema schema;
  /** The delimiter on the source file of the relation. */
  public Character delimiter;
  /** The indexes of the primary keys. Can support candidate keys. */
  public Set<Integer> keys;
  /** The indexes of foreign keys for the dimension tables. These keys correspond to indexes in the fact table.  */
  public Set<Integer> corresponding_fact_key;

  /**
   * @param relationKey the name of the table
   * @param type can be either a "fact" or "dimension" type
   * @param source the location of the table's data in S3
   * @param schema the schema of the table
   * @param delimiter the delimiter used on the table's data in S3
   * @param keys the column id of the primary key
   * @param corresponding_fact_key the column id of the corresponding foreign key, primarily only used for dimension tables
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
