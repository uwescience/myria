/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import java.util.Set;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.DataSource;

/**
 * 
 */
public class ParallelIngestEncoding extends MyriaApiEncoding {
  @Required
  public RelationKey relationKey;
  @Required
  public Schema schema;
  @Required
  public DataSource source;
  public Character delimiter;
  public Character quote;
  public Character escape;
  public Integer skip;
  public Set<Integer> workers;
}
