/**
 *
 */
package edu.washington.escience.myria.perfenforce.encoding;

import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.api.encoding.Required;
import edu.washington.escience.myria.io.AmazonS3Source;

/**
 * Table Description Encoding
 */
public class TableDescriptionEncoding {
  @Required
  public RelationKey relationKey;
  @Required
  public String type;
  @Required
  public AmazonS3Source source;
  @Required
  public Schema schema;
  @Required
  public Character delimiter;
  @Required
  public Set<Integer> keys;

  public Set<Integer> corresponding_fact_key;

  /**
   * @param relationKey2
   * @param type2
   * @param source2
   * @param schema2
   * @param delimiter2
   * @param keys2
   * @param corresponding_fact_key2
   */
  public TableDescriptionEncoding(final RelationKey relationKey, final String type, final AmazonS3Source source,
      final Schema schema, final Character delimiter, final Set<Integer> keys, final Set<Integer> corresponding_fact_key) {

    this.relationKey = relationKey;
    this.type = type;
    this.source = source;
    this.schema = schema;
    this.delimiter = delimiter;
    this.keys = keys;
    this.corresponding_fact_key = corresponding_fact_key;
  }
}
