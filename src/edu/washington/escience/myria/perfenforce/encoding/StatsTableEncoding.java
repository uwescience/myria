/**
 *
 */
package edu.washington.escience.myria.perfenforce.encoding;

import edu.washington.escience.myria.api.encoding.Required;

/**
 * Stats Table Encoding
 */
public class StatsTableEncoding {
  @Required
  public String table_name;
  @Required
  public long table_size;
  @Required
  public String selectivity_predicate_001;
  @Required
  public String selectivity_predicate_01;
  @Required
  public String selectivity_predicate_1;

  public StatsTableEncoding(final String table_name, final long table_size, final String selectivity_predicate_001,
      final String selectivity_predicate_01, final String selectivity_predicate_1) {
    this.table_name = table_name;
    this.table_size = table_size;
    this.selectivity_predicate_001 = selectivity_predicate_001;
    this.selectivity_predicate_01 = selectivity_predicate_01;
    this.selectivity_predicate_1 = selectivity_predicate_1;
  }
}