/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class encodes statistical information about a table from the user's schema.
 */
public class PerfEnforceStatisticsEncoding {
  /** The relation name */
  public String table_name;
  /** The relation size */
  public long table_size;
  /** The column value to obtain .001% of the data */
  public String selectivity_predicate_001;
  /**  The column value to obtain .01% of the data */
  public String selectivity_predicate_01;
  /**  The column value to obtain 1% of the data */
  public String selectivity_predicate_1;

  /**
   * Constructor for the PerfEnforceStatisticsEncoding encoding class.
   * @param table_name the name of the relation
   * @param table_size the size of the relation
   * @param selectivityList an array holding column values to obtain .001%, .01% and 1% of the relation
   */
  @JsonCreator
  public PerfEnforceStatisticsEncoding(
      @JsonProperty(value = "table_name", required = true) final String table_name,
      @JsonProperty(value = "table_size", required = true) final long table_size,
      @JsonProperty(value = "selectivityList", required = true)
      final List<String> selectivityList) {
    this.table_name = table_name;
    this.table_size = table_size;
    this.selectivity_predicate_001 = selectivityList.get(0);
    this.selectivity_predicate_01 = selectivityList.get(1);
    this.selectivity_predicate_1 = selectivityList.get(2);
  }
}
