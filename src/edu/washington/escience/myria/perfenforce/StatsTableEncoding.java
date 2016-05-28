/**
 *
 */
package edu.washington.escience.myria.perfenforce;

import edu.washington.escience.myria.api.encoding.Required;

/**
 * Stats Table Encoding
 */
public class StatsTableEncoding {
  @Required
  String table_name;
  @Required
  Integer table_size;
  @Required
  String selectivity_predicate_1;
  @Required
  String selectivity_predicate_10;
  @Required
  String selectivity_predicate_100;

  public StatsTableEncoding(final String table_name, final int table_size, final String selectivity_predicate_1,
      final String selectivity_predicate_10, final String selectivity_predicate_100) {
    this.table_name = table_name;
    this.table_size = table_size;
    this.selectivity_predicate_1 = selectivity_predicate_1;
    this.selectivity_predicate_1 = selectivity_predicate_10;
    this.selectivity_predicate_1 = selectivity_predicate_100;
  }
}