/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.AmazonS3Source;

/**
 * Table Description Encoding
 */
public class PerfEnforceTableEncoding {
  @Required public RelationKey relationKey;
  @Required public String type;
  @Required public AmazonS3Source source;
  @Required public Schema schema;
  @Required public Character delimiter;
  @Required public Set<Integer> keys;

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
  public PerfEnforceTableEncoding(
      final RelationKey relationKey,
      final String type,
      final AmazonS3Source source,
      final Schema schema,
      final Character delimiter,
      final Set<Integer> keys,
      final Set<Integer> corresponding_fact_key) {

    this.relationKey = relationKey;
    this.type = type;
    this.source = source;
    this.schema = schema;
    this.delimiter = delimiter;
    this.keys = keys;
    this.corresponding_fact_key = corresponding_fact_key;
  }
}
